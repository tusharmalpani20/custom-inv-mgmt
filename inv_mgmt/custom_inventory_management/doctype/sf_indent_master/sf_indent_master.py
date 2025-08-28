# Copyright (c) 2025, Hopnet Communications LLP and contributors
# For license information, please see license.txt

"""
SFIndentMaster DocType Controller

This module handles the business logic for SFIndentMaster documents, specifically focusing on
crate calculations and UOM (Unit of Measure) conversions.

Key Features:
1. Crate Calculation API
   - Calculates number of crates and loose items based on quantity
   - Uses UOM Conversion Detail to determine items per crate
   - Handles edge cases and errors gracefully

Implementation Notes:
- We query UOM Conversion Detail directly instead of going through Item -> UOM Conversion
  because it's more efficient and reduces database queries
- The "Crate" UOM is hardcoded because:
  a) It's a standard unit in the business process
  b) It's a fixed requirement in the inventory management system
  c) It ensures consistency across all calculations
  If this needs to be configurable in the future, we can move it to a system setting
"""

import frappe
from frappe.model.document import Document
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token

class SFIndentMaster(Document):
	def validate(self):
		"""
		Validate document before save - runs on every save
		"""
		# Always validate basic fields
		self.validate_basic_fields()
	
	def on_update(self):
		"""
		Runs after document is saved - good place for workflow state validation
		"""
		# Validate vehicle fields when workflow state changes to certain states
		self.validate_vehicle_fields_for_workflow()
	
	def on_submit(self):
		# Validate vehicle-related fields are mandatory
		self.validate_vehicle_fields()
		pass
		#self.create_draft_sales_order()

	def validate_basic_fields(self):
		"""
		Basic validation that should always run
		"""
		# Skip validation for adjusted indents as they inherit vehicle details from original indent
		if self.is_adjusted_indent:
			return
		
		# Basic validation - check if required fields are present
		if not self.delivery_route:
			frappe.throw("Delivery Route is mandatory")
		
		if not self.get("for"):
			frappe.throw("Plant/Warehouse is mandatory")
		
		if not self.date:
			frappe.throw("Date is mandatory")

	def validate_vehicle_fields_for_workflow(self):
		"""
		Validate vehicle fields based on workflow state
		This ensures validation happens at the right workflow transitions
		"""
		# Skip validation for adjusted indents
		if self.is_adjusted_indent:
			return
		
		# Get current workflow state
		current_state = self.workflow_state
		
		# Define states where vehicle validation should be enforced
		vehicle_required_states = [
			"Approved By Plant",
			"Delivery Started", 
			"Completed",
			"Submitted"
		]
		
		# CRITICAL: Prevent automatic transition to "Sent To Plant" if vehicle fields are missing
		if current_state == "Sent To Plant":
			# Check if this is a new document or if vehicle fields were just added
			if not self.vehicle or not self.vehicle_license_plate or not self.driver:
				frappe.throw(
					"Vehicle, Vehicle License Plate, and Driver are mandatory before sending to plant. "
					"Please fill in all vehicle details before proceeding.",
					title="Vehicle Details Required"
				)
		
		# If current state requires vehicle validation
		if current_state in vehicle_required_states:
			self.validate_vehicle_fields()
		
		# Additional validation for specific workflow states
		if current_state == "Approved By Plant":
			# Additional checks for plant approval
			self.validate_for_plant_approval()
		
		elif current_state == "Delivery Started":
			# Additional checks for delivery start
			self.validate_for_delivery_start()

	def validate_for_plant_approval(self):
		"""
		Additional validation when workflow state is 'Approved By Plant'
		"""
		# Ensure all items have quantities
		if not self.items:
			frappe.throw("At least one item must be added to the indent")
		
		for item in self.items:
			if not item.quantity or item.quantity <= 0:
				frappe.throw(f"Quantity must be greater than 0 for item {item.sku}")

	def validate_for_delivery_start(self):
		"""
		Additional validation when workflow state is 'Delivery Started'
		"""
		# Additional checks for delivery start if needed
		pass

	def validate_vehicle_fields(self):
		"""
		Validate that vehicle-related fields are mandatory when submitting
		"""
		# Skip validation for adjusted indents as they inherit vehicle details from original indent
		if self.is_adjusted_indent:
			return
		
		# Check if vehicle is selected
		if not self.vehicle:
			frappe.throw(
				"Vehicle is mandatory when submitting an indent. Please select a vehicle before submitting.",
				title="Vehicle Required"
			)
		
		# Check if vehicle license plate is provided
		if not self.vehicle_license_plate:
			frappe.throw(
				"Vehicle License Plate is mandatory when submitting an indent. Please enter the license plate number.",
				title="Vehicle License Plate Required"
			)
		
		# Check if driver is assigned
		if not self.driver:
			frappe.throw(
				"Driver is mandatory when submitting an indent. Please assign a driver before submitting.",
				title="Driver Required"
			)
		
		# Additional validation: Check if driver is active
		if self.driver:
			driver_status = frappe.get_value("Employee", self.driver, "status")
			if driver_status != "Active":
				frappe.throw(
					f"Driver {self.driver} is not active (Status: {driver_status}). Please assign an active driver.",
					title="Inactive Driver"
				)
		
		# Note: Vehicle status validation removed as Vehicle doctype doesn't have a status field
		# If vehicle status validation is needed in the future, a custom status field should be added to Vehicle doctype

	def before_save(self):
		"""
		Runs before document is saved - prevent automatic workflow transitions
		"""
		# Skip validation for adjusted indents
		if self.is_adjusted_indent:
			return
		
		# If this is a new document or being saved for the first time
		if not self.name or frappe.db.exists("SF Indent Master", self.name) == False:
			# For new documents, always start in Draft state
			self.workflow_state = "Draft"
			frappe.msgprint(
				"Document saved in Draft state. Fill in vehicle details and use 'Send To Plant' action when ready.",
				indicator="blue",
				alert=True
			)
		
		# For existing documents in Draft state, check if trying to transition to Sent To Plant
		elif self.workflow_state == "Draft":
			# Check if vehicle fields are missing and prevent transition to "Sent To Plant"
			if not self.vehicle or not self.vehicle_license_plate or not self.driver:
				frappe.msgprint(
					"Vehicle details are missing. Please fill in Vehicle, Vehicle License Plate, and Driver before using 'Send To Plant' action.",
					indicator="yellow",
					alert=True
				)
			else:
				# Vehicle details are complete, can proceed to "Sent To Plant" via workflow action
				frappe.msgprint(
					"Vehicle details are complete. You can now use 'Send To Plant' action.",
					indicator="green",
					alert=True
				)
	
	@frappe.whitelist()
	def pre_populate_indent_items(self):
		"""
		Pre-populate SF Indent Master with all available items
		Called from Fetch Items button.
		"""
		try:
			# Remove any existing rows where SKU is not set
			if self.items:
				# Create a list of items to remove (those without SKU)
				items_to_remove = []
				for i, item in enumerate(self.items):
					if not item.sku:
						items_to_remove.append(i)
				
				# Remove items in reverse order to maintain correct indices
				for i in reversed(items_to_remove):
					self.items.pop(i)
			
			# Get all items sorted by name
			items = frappe.get_all("Item", 
				filters={"has_variants": 0, "disabled": 0, "is_stock_item": 1},
				fields=["item_code", "item_name", "stock_uom"],
				limit_page_length=10000,
				ignore_permissions=True,
				order_by="item_name asc"
			)
			
			# Get existing SKUs to avoid duplicates
			existing_skus = []
			if self.items:
				existing_skus = [item.sku for item in self.items if item.sku]
			
			added_count = 0
			skipped_count = 0
			removed_count = len(items_to_remove) if 'items_to_remove' in locals() else 0
			
			# Process each item to get crate details with default quantity 0
			for item in items:
				# Skip if item already exists
				if item.item_code in existing_skus:
					skipped_count += 1
					continue
					
				# Get crate details for quantity 0
				crate_details = get_crate_details_for_item(item.item_code, 0)
				
				# Add new item to the table
				new_item = self.append('items', {})
				new_item.sku = item.item_code
				new_item.sku_name = item.item_name
				new_item.uom = item.stock_uom
				new_item.quantity = 0
				new_item.crates = crate_details.get("crates", 0)
				new_item.loose = crate_details.get("loose", 0)
				new_item.difference = 0
				new_item.actual = 0
				
				added_count += 1
			
			# Show success message
			message = f"Added {added_count} new items"
			if removed_count > 0:
				message += f", removed {removed_count} rows without SKU"
			if skipped_count > 0:
				message += f", skipped {skipped_count} existing items"
			
			frappe.msgprint(message, alert=True)
			
		except Exception as e:
			frappe.log_error(f"Error pre-populating indent items: {str(e)}", "Pre-populate Indent Items Error")
			frappe.throw(f"Error fetching items: {str(e)}")


	# def create_draft_sales_order(self):
	# 	"""Create a draft Sales Order from the SFIndentMaster"""
	# 	# Get delivery route and points
	# 	delivery_route = frappe.get_doc("SF Delivery Route Master", self.delivery_route)
		
	# 	# Find first customer with Distribution Center category
	# 	distribution_customer = None
	# 	for point in delivery_route.delivery_points:
	# 		customer = frappe.get_doc("Customer", point.customer)
	# 		if customer.custom_customer_category == "Distribution Center":
	# 			distribution_customer = customer
	# 			break
		
	# 	if not distribution_customer:
	# 		frappe.throw("No Distribution Center customer found in delivery points")

	# 	# Create Sales Order
	# 	sales_order = frappe.new_doc("Sales Order")
	# 	sales_order.update({
	# 		"customer": distribution_customer.name,
	# 		"delivery_date": self.get("date"),
	# 		"company": self.company,
	# 		"set_warehouse": self.get("for"),
	# 		"transaction_date": self.get("date"),
	# 		"delivery_route": self.delivery_route,
	# 		"sf_indent_master": self.name
	# 	})

	# 	# Copy items from SFIndentMaster
	# 	for item in self.items:
	# 		# Get default rate from Item master
	# 		item_doc = frappe.get_doc("Item", item.sku)
	# 		default_rate = item_doc.standard_rate or 0  # Use standard_rate as default, fallback to 0
			
	# 		sales_order.append("items", {
	# 			"item_code": item.sku,
	# 			"qty": item.quantity,
	# 			"rate": default_rate,
	# 			"warehouse": self.get("for"),  # Use get() method to safely access the field
	# 			"delivery_date": self.get("date"),
	# 			# "uom": item.uom,
	# 			# "conversion_factor": item.conversion_factor,
	# 			# "stock_uom": item.stock_uom,
	# 			# "stock_qty": item.stock_qty,
	# 			"qty": item.actual,
	# 			# "description": item.description
	# 		})

	# 	sales_order.insert()
	# 	sales_order.submit()
		
	# 	# Link the Sales Order to the SFIndentMaster
	# 	self.db_set("sales_order", sales_order.name)
	# 	self.db_set("customer", sales_order.customer)
	# 	frappe.msgprint(f"Draft Sales Order {sales_order.name} has been created successfully")


@frappe.whitelist()
def get_crate_details(sku, quantity):
	"""
	Get crate details for a given SKU and quantity.
	
	This function calculates how many complete crates and loose items can be made
	from a given quantity, based on the UOM conversion factor.
	
	Args:
		sku (str): The item code/SKU to check
		quantity (float): The total quantity to calculate crates for
	
	Returns:
		dict: A dictionary containing:
			- crates: Number of complete crates (0 if no crate conversion)
			- loose: Number of items that don't fit in complete crates (or total quantity if no crate conversion)
			- conversion_factor: Number of items per crate (0 if no crate conversion)
			- has_crate_conversion: Whether the item has crate conversion defined
			- actual: Same as quantity (for items without crate conversion)
			- message: Information message for user
	
	Example:
		If an item has 24 items per crate and quantity is 100:
		- crates will be 4 (100 // 24)
		- loose will be 4 (100 - (4 * 24))
		
		If an item has no crate conversion and quantity is 100:
		- crates will be 0
		- loose will be 100
		- actual will be 100
	"""
	try:
		quantity = float(quantity)
		
		# Get UOM conversion detail directly
		crate_conversion = frappe.get_all(
			"UOM Conversion Detail",
			filters={
				"parent": sku,
				"uom": "Crate"  # Hardcoded as per business requirement
			},
			fields=["conversion_factor"],
			limit=1
		)
		
		if crate_conversion:
			conversion_factor = crate_conversion[0].conversion_factor
			
			# Calculate crates and loose
			crates = int(quantity // conversion_factor)  # Integer division for whole crates
			loose = quantity - (crates * conversion_factor)  # Remainder is loose items
			
			return {
				'crates': crates,
				'loose': loose,
				'conversion_factor': conversion_factor,
				'has_crate_conversion': True,
				'actual': quantity,
				'message': None
			}
		
		# For items without crate conversion
		return {
			'crates': 0,
			'loose': quantity,  # All quantity goes to loose items
			'conversion_factor': 0,
			'has_crate_conversion': False,
			'actual': quantity,  # Set actual same as quantity
			'message': f"No crate conversion found for item {sku}. All quantity will be treated as loose items."
		}
		
	except Exception as e:
		frappe.log_error(frappe.get_traceback(), "SFIndentMaster Crate Calculation Error")
		return {
			'error': str(e),
			'has_crate_conversion': False,
			'message': f"Error calculating crates: {str(e)}"
		}



def get_crate_details_for_item(sku, quantity):
    """
    Get crate details for a specific item and quantity
    
    Args:
        sku: Item code
        quantity: Quantity to calculate for
    
    Returns:
        Dictionary with crates, loose, and actual quantities
    """
    try:
        # Call the existing crate calculation function
        result = get_crate_details(sku, quantity)
        
        if isinstance(result, dict) and "error" not in result:
            return result
        else:
            # Return default values if calculation fails
            return {
                "crates": 0,
                "loose": quantity,
                "actual": quantity
            }
            
    except Exception as e:
        frappe.log_error(f"Error getting crate details for {sku}: {str(e)}", "Crate Details Error")
        # Return default values as fallback
        return {
            "crates": 0,
            "loose": quantity,
            "actual": quantity
        }