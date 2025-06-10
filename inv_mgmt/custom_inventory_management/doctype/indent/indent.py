# Copyright (c) 2025, Hopnet Communications LLP and contributors
# For license information, please see license.txt

"""
Indent DocType Controller

This module handles the business logic for Indent documents, specifically focusing on
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

class Indent(Document):
	def on_submit(self):
		pass
		#self.create_draft_sales_order()

	# def create_draft_sales_order(self):
	# 	"""Create a draft Sales Order from the Indent"""
	# 	# Get delivery route and points
	# 	delivery_route = frappe.get_doc("Delivery Route", self.delivery_route)
		
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
	# 		"indent": self.name
	# 	})

	# 	# Copy items from Indent
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
		
	# 	# Link the Sales Order to the Indent
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
		frappe.log_error(frappe.get_traceback(), "Indent Crate Calculation Error")
		return {
			'error': str(e),
			'has_crate_conversion': False,
			'message': f"Error calculating crates: {str(e)}"
		}
