import frappe
from frappe import _
from datetime import datetime
import base64
import json
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token, handle_error_response
from typing import Dict, Any
from frappe.utils import flt

@frappe.whitelist(allow_guest=True, methods=["PUT"])
def acknowledge_delivery_note() -> Dict[str, Any]:
    """
    Updates delivery note with receiver's signature and acknowledgment time, then changes workflow state to Delivered
    Required header: Authorization Bearer token
    Required body params:
        delivery_note: str - The delivery note ID
        signature: str - Base64 encoded signature image
    Returns:
        Dict containing success/error information
    """
    # Start transaction
    frappe.db.begin()
    
    try:
        # Log incoming request
        frappe.log_error(
            title="Delivery Note Acknowledgment - Request",
            message=f"Headers: {frappe.request.headers}\nData: {frappe.request.get_json()}"
        )

        # Verify authorization
        is_valid, result = verify_dp_token(frappe.request.headers)
        if not is_valid:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 401
            return result
        
        employee = result["employee"]
        
        # Get request data
        data = frappe.request.get_json()
        
        if not data:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "No data provided",
                "code": "NO_DATA",
                "http_status_code": 400
            }
        
        delivery_note_id = data.get('delivery_note')
        signature_base64 = data.get('signature')
        
        if not delivery_note_id or not signature_base64:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "delivery_note and signature are required fields",
                "code": "MISSING_FIELDS",
                "http_status_code": 400
            }
        
        # Get delivery note document
        try:
            delivery_note = frappe.get_doc("Delivery Note", delivery_note_id)
        except frappe.DoesNotExistError:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": f"Delivery Note {delivery_note_id} not found",
                "code": "DELIVERY_NOTE_NOT_FOUND",
                "http_status_code": 404
            }

        # Check if already submitted
        if delivery_note.docstatus == 1:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": f"Delivery Note {delivery_note_id} is already submitted",
                "code": "ALREADY_SUBMITTED",
                "http_status_code": 400
            }

        # Verify if the delivery note is already acknowledged
        if delivery_note.custom_receiver_signature:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": f"Delivery Note {delivery_note_id} is already acknowledged",
                "code": "ALREADY_ACKNOWLEDGED",
                "http_status_code": 400
            }

        # Handle signature image
        try:
            # Remove data URL prefix if present
            if signature_base64.startswith('data:'):
                signature_base64 = signature_base64.split('base64,')[1].strip()
            
            # Decode base64 string
            try:
                file_content = base64.b64decode(signature_base64)
            except Exception as e:
                frappe.log_error(
                    title="Signature Processing - Base64 Decode Failed",
                    message=f"Error: {str(e)}"
                )
                raise frappe.ValidationError("Invalid base64 signature data")

            # Generate filename with timestamp
            filename = f"signature_{delivery_note_id}_{frappe.utils.now_datetime().strftime('%Y%m%d%H%M%S')}.png"
            
            # Create file doc
            file_doc = frappe.get_doc({
                "doctype": "File",
                "file_name": filename,
                "content": file_content,
                "is_private": 0,
                "attached_to_doctype": "Delivery Note",
                "attached_to_name": delivery_note_id
            })
            file_doc.insert()
            
            # Log successful file creation
            frappe.log_error(
                title="Delivery Note Acknowledgment - File Created",
                message=f"File: {file_doc.name}\nURL: {file_doc.file_url}"
            )

        except Exception as e:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": f"Failed to process signature: {str(e)}",
                "code": "SIGNATURE_PROCESSING_FAILED",
                "http_status_code": 400
            }
            
        try:
            # Update delivery note
            current_time = frappe.utils.now_datetime()
            
            # Apply workflow action
            frappe.model.workflow.apply_workflow(delivery_note, "Deliver")
            
            # Update additional fields
            delivery_note.db_set('custom_receiver_signature', file_doc.file_url)
            delivery_note.db_set('custom_receiving_time', current_time)
            
            # Commit transaction
            frappe.db.commit()
            
            frappe.local.response['http_status_code'] = 200
            return {
                "success": True,
                "status": "success",
                "message": "Delivery note acknowledged successfully",
                "code": "ACKNOWLEDGMENT_SAVED",
                "data": {
                    "delivery_note": delivery_note_id,
                    "signature_url": file_doc.file_url,
                    "receiving_time": str(current_time),
                    "workflow_state": delivery_note.workflow_state
                },
                "http_status_code": 200
            }

        except frappe.exceptions.WorkflowTransitionError as e:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": str(e),
                "code": "WORKFLOW_TRANSITION_ERROR",
                "http_status_code": 400
            }
        except Exception as e:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 500
            return {
                "success": False,
                "status": "error",
                "message": f"Failed to acknowledge delivery note: {str(e)}",
                "code": "ACKNOWLEDGMENT_FAILED",
                "http_status_code": 500
            }

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(
            title="Delivery Note Acknowledgment Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        frappe.local.response['http_status_code'] = 500
        return handle_error_response(e, "Error processing delivery note acknowledgment")


@frappe.whitelist(allow_guest=True, methods=["GET"])
def get_delivery_note_details(delivery_note: str) -> Dict[str, Any]:
    """
    Get delivery note details with crate calculations for items
    Required header: Authorization Bearer token
    Required params:
        delivery_note: str - The delivery note ID
    Returns:
        Dict containing delivery note details and items with crate calculations
    """
    try:
        # Verify authorization
        is_valid, result = verify_dp_token(frappe.request.headers)
        if not is_valid:
            frappe.local.response['http_status_code'] = 401
            return result

        # Get delivery note document
        try:
            delivery_note_doc = frappe.get_doc("Delivery Note", delivery_note)
        except frappe.DoesNotExistError:
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": f"Delivery Note {delivery_note} not found",
                "code": "DELIVERY_NOTE_NOT_FOUND",
                "http_status_code": 404
            }

        # Get all item codes from delivery note items
        item_codes = [item.item_code for item in delivery_note_doc.items]

        # Fetch crate conversion factors for all items in one query
        crate_conversions = frappe.get_all(
            "UOM Conversion Detail",
            filters={
                "parent": ["in", item_codes],
                "uom": "Crate"
            },
            fields=["parent", "conversion_factor"],
            as_list=False
        )

        # Create a mapping of item_code to conversion factor
        conversion_map = {
            conv.parent: conv.conversion_factor 
            for conv in crate_conversions
        }

        # Process items with crate calculations
        items = []
        for item in delivery_note_doc.items:
            conversion_factor = conversion_map.get(item.item_code, 0)
            qty = float(item.qty)

            if conversion_factor:
                crates = int(qty // conversion_factor)
                loose = qty - (crates * conversion_factor)
            else:
                crates = 0
                loose = qty

            items.append({
                "item_code": item.item_code,
                "item_name": item.item_name,
                "qty": qty,
                "stock_uom": item.stock_uom,
                "crates": crates,
                "loose": loose,
                "has_crate_conversion": bool(conversion_factor),
                "conversion_factor": conversion_factor
            })

        # Prepare response
        response = {
            "success": True,
            "status": "success",
            "message": "Delivery note details fetched successfully",
            "code": "DETAILS_FETCHED",
            "data": {
                "delivery_note": delivery_note,
                "customer": delivery_note_doc.customer,
                "customer_name": delivery_note_doc.customer_name,
                "posting_date": str(delivery_note_doc.posting_date),
                "status": delivery_note_doc.status,
                "docstatus": delivery_note_doc.docstatus,
                "workflow_state": delivery_note_doc.workflow_state,
                "items": items
            },
            "http_status_code": 200
        }

        frappe.local.response['http_status_code'] = 200
        return response

    except Exception as e:
        frappe.log_error(
            title="Get Delivery Note Details Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        frappe.local.response['http_status_code'] = 500
        return handle_error_response(e, "Error fetching delivery note details")


@frappe.whitelist(allow_guest=True, methods=["POST"])
def create_delivery_note_from_sales_order():
	"""
	Create a delivery note from sales order with signature and custom validations
	
	Required header: Authorization Bearer token
	Required body params:
		sales_order_id: str - Sales Order ID
		signature: str - Base64 encoded signature image (optional)
		items: list - Array of items with item_code and qty
			[
				{
					"item_code": "ITEM001",
					"qty": 10
				}
			]
	
	Returns:
		dict: Success/error response with delivery note details
	"""
	# Start transaction
	frappe.db.begin()
	
	try:
		# Verify authorization
		is_valid, result = verify_dp_token(frappe.request.headers)
		if not is_valid:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 401
			return result
		
		employee_id = result["employee"]
		
		# Get request data
		data = frappe.request.get_json()
		
		if not data:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "No data provided",
				"code": "NO_DATA",
				"http_status_code": 400
			}
		
		sales_order_id = data.get("sales_order_id")
		signature_base64 = data.get("signature")
		requested_items = data.get("items", [])
		
		if not sales_order_id:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "Sales Order ID is required",
				"code": "MISSING_SALES_ORDER_ID",
				"http_status_code": 400
			}
		
		if not requested_items:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "Items are required",
				"code": "MISSING_ITEMS",
				"http_status_code": 400
			}
		
		# Get sales order
		try:
			sales_order = frappe.get_doc("Sales Order", sales_order_id)
		except frappe.DoesNotExistError:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 404
			return {
				"success": False,
				"status": "error",
				"message": f"Sales Order {sales_order_id} not found",
				"code": "SALES_ORDER_NOT_FOUND",
				"http_status_code": 404
			}
		
		if sales_order.docstatus != 1:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "Sales Order must be submitted",
				"code": "SALES_ORDER_NOT_SUBMITTED",
				"http_status_code": 400
			}
		
		# Get driver record for the authenticated employee
		driver_record = frappe.db.get_value(
			"Driver",
			{"employee": employee_id, "status": "Active"},
			"name"
		)
		
		vehicle_no = None
		if driver_record:
			# Get first vehicle assigned to the driver
			vehicle = frappe.db.sql("""
				SELECT name, license_plate
				FROM `tabVehicle`
				WHERE custom_driver = %(driver)s
				ORDER BY creation ASC
				LIMIT 1
			""", {"driver": driver_record}, as_dict=True)
			
			if vehicle:
				vehicle_no = vehicle[0].license_plate or vehicle[0].name
		
		# Check if delivery note already exists for this sales order with same driver and vehicle
		# Query through Delivery Note Item table since against_sales_order is on items, not on the main document
		existing_dn_ids = frappe.db.sql("""
			SELECT DISTINCT parent 
			FROM `tabDelivery Note Item` 
			WHERE against_sales_order = %s
		""", sales_order_id, as_list=True)
		
		existing_dn = None
		if existing_dn_ids:
			# Get the first delivery note that matches our criteria
			for dn_id in existing_dn_ids:
				dn_name = dn_id[0]
				dn_doc = frappe.get_doc("Delivery Note", dn_name)
				
				# Check if existing DN has same driver and vehicle
				if (dn_doc.driver == driver_record and 
					dn_doc.vehicle_no == vehicle_no):
					existing_dn = dn_name
					break
			
			# If no matching driver/vehicle found, check if any non-cancelled DN exists
			if not existing_dn:
				for dn_id in existing_dn_ids:
					dn_name = dn_id[0]
					dn_doc = frappe.get_doc("Delivery Note", dn_name)
					if dn_doc.docstatus != 2:  # Not cancelled
						existing_dn = dn_name
						break
		
		if existing_dn:
			# Get the existing delivery note to check driver and vehicle
			existing_dn_doc = frappe.get_doc("Delivery Note", existing_dn)
			
			# Check if existing DN has same driver and vehicle
			if (existing_dn_doc.driver == driver_record and 
				existing_dn_doc.vehicle_no == vehicle_no):
				# Skip creation if same driver and vehicle
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": f"Delivery Note already exists with same driver and vehicle: {existing_dn}",
					"code": "DUPLICATE_DELIVERY_NOTE",
					"http_status_code": 400
				}
			else:
				# Different driver/vehicle, check if we can create new one
				if existing_dn_doc.docstatus != 2:  # Not cancelled
					frappe.db.rollback()
					frappe.local.response['http_status_code'] = 400
					return {
						"success": False,
						"status": "error",
						"message": f"Delivery Note already exists and is not cancelled: {existing_dn}",
						"code": "DELIVERY_NOTE_EXISTS",
						"http_status_code": 400
					}
		
		# Validate requested quantities against available quantities
		so_items_map = {item.item_code: item for item in sales_order.items}
		validated_items = []
		
		for req_item in requested_items:
			item_code = req_item.get("item_code")
			requested_qty = flt(req_item.get("qty", 0))
			
			if not item_code:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": "Item code is required for all items",
					"code": "MISSING_ITEM_CODE",
					"http_status_code": 400
				}
			
			if requested_qty <= 0:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": f"Quantity must be greater than 0 for item {item_code}",
					"code": "INVALID_QUANTITY",
					"http_status_code": 400
				}
			
			if item_code not in so_items_map:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": f"Item {item_code} not found in Sales Order {sales_order_id}",
					"code": "ITEM_NOT_IN_SALES_ORDER",
					"http_status_code": 400
				}
			
			so_item = so_items_map[item_code]
			pending_qty = flt(so_item.qty) - flt(so_item.delivered_qty)
			
			if requested_qty > pending_qty:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": f"Requested quantity {requested_qty} for item {item_code} exceeds pending quantity {pending_qty}",
					"code": "QUANTITY_EXCEEDS_PENDING",
					"http_status_code": 400
				}
			
			validated_items.append({
				"item_code": item_code,
				"qty": requested_qty,
				"so_item": so_item
			})
		
		# Check target warehouse requirement for internal customers before creating delivery note
		if sales_order.is_internal_customer:
			target_warehouse = sales_order.get("custom_set_target_warehouse")
			if not target_warehouse:
				# Try to get from company default warehouse
				target_warehouse = frappe.db.get_value(
					"Warehouse", 
					{"is_group": 0, "company": sales_order.company}, 
					"name"
				)
			
			if not target_warehouse:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": f"Target Warehouse is mandatory for internal transfers. Sales Order {sales_order_id} does not have custom_set_target_warehouse set and no default warehouse found.",
					"code": "MISSING_TARGET_WAREHOUSE",
					"http_status_code": 400
				}
		
		# Create delivery note
		delivery_note = frappe.new_doc("Delivery Note")
		
		# Copy header fields from sales order
		delivery_note.customer = sales_order.customer
		delivery_note.customer_name = sales_order.customer_name
		delivery_note.customer_address = sales_order.customer_address
		delivery_note.address_display = sales_order.address_display
		delivery_note.contact_person = sales_order.contact_person
		delivery_note.contact_display = sales_order.contact_display
		delivery_note.contact_email = sales_order.contact_email
		delivery_note.contact_mobile = sales_order.contact_mobile
		delivery_note.currency = sales_order.currency
		delivery_note.conversion_rate = sales_order.conversion_rate
		delivery_note.selling_price_list = sales_order.selling_price_list
		delivery_note.price_list_currency = sales_order.price_list_currency
		delivery_note.plc_conversion_rate = sales_order.plc_conversion_rate
		delivery_note.ignore_pricing_rule = sales_order.ignore_pricing_rule
		delivery_note.company = sales_order.company
		delivery_note.project = sales_order.project
		delivery_note.cost_center = sales_order.cost_center
		delivery_note.territory = sales_order.territory
		delivery_note.customer_group = sales_order.customer_group
		delivery_note.is_internal_customer = sales_order.is_internal_customer
		delivery_note.represents_company = sales_order.represents_company
		delivery_note.set_warehouse = sales_order.set_warehouse
		
		# Set driver and vehicle if found
		if driver_record:
			delivery_note.driver = driver_record
		if vehicle_no:
			delivery_note.vehicle_no = vehicle_no
		
		# Handle internal customer warehouse settings
		if sales_order.is_internal_customer:
			delivery_note.set_target_warehouse = target_warehouse
		
		# Add items to delivery note
		for validated_item in validated_items:
			so_item = validated_item["so_item"]
			dn_item = delivery_note.append("items", {})
			
			# Copy item fields from sales order item
			dn_item.item_code = so_item.item_code
			dn_item.item_name = so_item.item_name
			dn_item.description = so_item.description
			dn_item.qty = validated_item["qty"]
			dn_item.uom = so_item.uom
			dn_item.conversion_factor = so_item.conversion_factor
			dn_item.stock_qty = flt(validated_item["qty"]) * flt(so_item.conversion_factor)
			dn_item.rate = so_item.rate
			dn_item.amount = flt(validated_item["qty"]) * flt(so_item.rate)
			dn_item.base_rate = so_item.base_rate
			dn_item.base_amount = flt(validated_item["qty"]) * flt(so_item.base_rate)
			dn_item.warehouse = so_item.warehouse
			
			# Set target warehouse on item level for internal customers
			if sales_order.is_internal_customer:
				dn_item.target_warehouse = target_warehouse
			
			dn_item.against_sales_order = sales_order.name
			dn_item.so_detail = so_item.name
			dn_item.cost_center = so_item.cost_center
			dn_item.project = so_item.project
			
			# Copy other relevant fields
			if hasattr(so_item, 'item_group'):
				dn_item.item_group = so_item.item_group
			if hasattr(so_item, 'brand'):
				dn_item.brand = so_item.brand
		
		# Copy taxes from sales order if any
		for tax in sales_order.taxes:
			dn_tax = delivery_note.append("taxes", {})
			dn_tax.charge_type = tax.charge_type
			dn_tax.account_head = tax.account_head
			dn_tax.rate = tax.rate
			dn_tax.description = tax.description
			dn_tax.cost_center = tax.cost_center
		
		# Set missing values and calculate totals
		delivery_note.run_method("set_missing_values")
		delivery_note.run_method("calculate_taxes_and_totals")
		
		# Insert the delivery note
		delivery_note.insert()
		
		# Handle signature if provided
		signature_file_url = None
		if signature_base64:
			try:
				# Remove data URL prefix if present
				if signature_base64.startswith('data:'):
					signature_base64 = signature_base64.split('base64,')[1].strip()
				
				# Decode base64 string
				file_content = base64.b64decode(signature_base64)
				
				# Generate filename with timestamp
				filename = f"signature_{delivery_note.name}_{frappe.utils.now_datetime().strftime('%Y%m%d%H%M%S')}.png"
				
				# Create file doc
				file_doc = frappe.get_doc({
					"doctype": "File",
					"file_name": filename,
					"content": file_content,
					"is_private": 0,
					"attached_to_doctype": "Delivery Note",
					"attached_to_name": delivery_note.name
				})
				file_doc.insert()
				signature_file_url = file_doc.file_url
				
			except Exception as e:
				frappe.log_error(
					title="Delivery Note Creation - Signature Processing Failed",
					message=f"Delivery Note: {delivery_note.name}\nError: {str(e)}"
				)
				# Don't fail the entire operation for signature issues
		
		# Commit transaction
		frappe.db.commit()
		
		frappe.local.response['http_status_code'] = 201
		return {
			"success": True,
			"status": "success",
			"message": f"Delivery Note {delivery_note.name} created successfully",
			"delivery_note": delivery_note.name,
			"signature_file_url": signature_file_url,
			"data": {
				"name": delivery_note.name,
				"customer": delivery_note.customer,
				"customer_name": delivery_note.customer_name,
				"total_qty": delivery_note.total_qty,
				"grand_total": delivery_note.grand_total,
				"driver": delivery_note.driver,
				"vehicle_no": delivery_note.vehicle_no,
				"posting_date": delivery_note.posting_date,
				"posting_time": delivery_note.posting_time
			},
			"http_status_code": 201
		}
		
	except Exception as e:
		frappe.db.rollback()
		frappe.log_error(
			title="Delivery Note Creation Failed",
			message=f"Employee: {employee_id if 'employee_id' in locals() else 'Unknown'}\nError: {str(e)}\nTraceback: {frappe.get_traceback()}"
		)
		
		frappe.local.response['http_status_code'] = 500
		return {
			"success": False,
			"status": "error",
			"message": str(e),
			"code": "DELIVERY_NOTE_CREATION_FAILED",
			"http_status_code": 500
		}

@frappe.whitelist(allow_guest=True, methods=["POST"])
def start_sales_order_delivery():
	"""
	Create draft delivery notes from multiple sales orders
	
	Required header: Authorization Bearer token
	Required body params:
		sales_order_ids: list - Array of Sales Order IDs
			[
				"SO-001",
				"SO-002"
			]
	
	Returns:
		dict: Success/error response with created delivery note details
	"""
	# Start transaction
	frappe.db.begin()
	
	try:
		# Verify authorization
		is_valid, result = verify_dp_token(frappe.request.headers)
		if not is_valid:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 401
			return result
		
		employee_id = result["employee"]
		
		# Get request data
		data = frappe.request.get_json()
		
		if not data:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "No data provided",
				"code": "NO_DATA",
				"http_status_code": 400
			}
		
		sales_order_ids = data.get("sales_order_ids", [])
		
		if not sales_order_ids:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "Sales Order IDs are required",
				"code": "MISSING_SALES_ORDER_IDS",
				"http_status_code": 400
			}
		
		if not isinstance(sales_order_ids, list):
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "sales_order_ids must be a list",
				"code": "INVALID_SALES_ORDER_IDS_FORMAT",
				"http_status_code": 400
			}
		
		# Get driver record for the authenticated employee
		driver_record = frappe.db.get_value(
			"Driver",
			{"employee": employee_id, "status": "Active"},
			"name"
		)
		
		vehicle_no = None
		if driver_record:
			# Get first vehicle assigned to the driver
			vehicle = frappe.db.sql("""
				SELECT name, license_plate
				FROM `tabVehicle`
				WHERE custom_driver = %(driver)s
				ORDER BY creation ASC
				LIMIT 1
			""", {"driver": driver_record}, as_dict=True)
			
			if vehicle:
				vehicle_no = vehicle[0].license_plate or vehicle[0].name
		
		created_delivery_notes = []
		failed_sales_orders = []
		
		# Process each sales order
		for sales_order_id in sales_order_ids:
			try:
				# Get sales order
				try:
					sales_order = frappe.get_doc("Sales Order", sales_order_id)
				except frappe.DoesNotExistError:
					failed_sales_orders.append({
						"sales_order_id": sales_order_id,
						"error": "Sales Order not found"
					})
					continue
				
				if sales_order.docstatus != 1:
					failed_sales_orders.append({
						"sales_order_id": sales_order_id,
						"error": "Sales Order must be submitted"
					})
					continue
				
				# Check if delivery note already exists for this sales order
				# Query through Delivery Note Item table since against_sales_order is on items, not on the main document
				existing_dn_ids = frappe.db.sql("""
					SELECT DISTINCT parent 
					FROM `tabDelivery Note Item` 
					WHERE against_sales_order = %s
				""", sales_order_id, as_list=True)
				
				existing_dn = None
				if existing_dn_ids:
					# Get the first delivery note that matches our criteria
					for dn_id in existing_dn_ids:
						dn_name = dn_id[0]
						dn_doc = frappe.get_doc("Delivery Note", dn_name)
						
						# Check if existing DN has same driver and vehicle
						if (dn_doc.driver == driver_record and 
							dn_doc.vehicle_no == vehicle_no):
							existing_dn = dn_name
							break
					
					# If no matching driver/vehicle found, check if any non-cancelled DN exists
					if not existing_dn:
						for dn_id in existing_dn_ids:
							dn_name = dn_id[0]
							dn_doc = frappe.get_doc("Delivery Note", dn_name)
							if dn_doc.docstatus != 2:  # Not cancelled
								existing_dn = dn_name
								break
				
				if existing_dn:
					# Get the existing delivery note to check driver and vehicle
					existing_dn_doc = frappe.get_doc("Delivery Note", existing_dn)
					
					# Check if existing DN has same driver and vehicle
					if (existing_dn_doc.driver == driver_record and 
						existing_dn_doc.vehicle_no == vehicle_no):
						# Skip creation if same driver and vehicle
						failed_sales_orders.append({
							"sales_order_id": sales_order_id,
							"error": f"Delivery Note already exists with same driver and vehicle: {existing_dn}"
						})
						continue
					else:
						# Different driver/vehicle, check if we can create new one
						if existing_dn_doc.docstatus != 2:  # Not cancelled
							failed_sales_orders.append({
								"sales_order_id": sales_order_id,
								"error": f"Delivery Note already exists and is not cancelled: {existing_dn}"
							})
							continue
				
				# Check target warehouse requirement for internal customers before creating delivery note
				if sales_order.is_internal_customer:
					target_warehouse = sales_order.get("custom_set_target_warehouse")
					if not target_warehouse:
						# Try to get from company default warehouse
						target_warehouse = frappe.db.get_value(
							"Warehouse", 
							{"is_group": 0, "company": sales_order.company}, 
							"name"
						)
					
					if not target_warehouse:
						failed_sales_orders.append({
							"sales_order_id": sales_order_id,
							"error": "Target Warehouse is mandatory for internal transfers. Sales Order does not have custom_set_target_warehouse set and no default warehouse found."
						})
						continue
				
				# Create delivery note
				delivery_note = frappe.new_doc("Delivery Note")
				
				# Copy header fields from sales order
				delivery_note.customer = sales_order.customer
				delivery_note.customer_name = sales_order.customer_name
				delivery_note.customer_address = sales_order.customer_address
				delivery_note.address_display = sales_order.address_display
				delivery_note.contact_person = sales_order.contact_person
				delivery_note.contact_display = sales_order.contact_display
				delivery_note.contact_email = sales_order.contact_email
				delivery_note.contact_mobile = sales_order.contact_mobile
				delivery_note.currency = sales_order.currency
				delivery_note.conversion_rate = sales_order.conversion_rate
				delivery_note.selling_price_list = sales_order.selling_price_list
				delivery_note.price_list_currency = sales_order.price_list_currency
				delivery_note.plc_conversion_rate = sales_order.plc_conversion_rate
				delivery_note.ignore_pricing_rule = sales_order.ignore_pricing_rule
				delivery_note.company = sales_order.company
				delivery_note.project = sales_order.project
				delivery_note.cost_center = sales_order.cost_center
				delivery_note.territory = sales_order.territory
				delivery_note.customer_group = sales_order.customer_group
				delivery_note.is_internal_customer = sales_order.is_internal_customer
				delivery_note.represents_company = sales_order.represents_company
				delivery_note.set_warehouse = sales_order.set_warehouse
				
				# Set driver and vehicle if found
				if driver_record:
					delivery_note.driver = driver_record
				if vehicle_no:
					delivery_note.vehicle_no = vehicle_no
				
				# Handle internal customer warehouse settings
				if sales_order.is_internal_customer:
					delivery_note.set_target_warehouse = target_warehouse
				
				# Add all items from sales order with pending quantities
				for so_item in sales_order.items:
					pending_qty = flt(so_item.qty) - flt(so_item.delivered_qty)
					
					if pending_qty > 0:
						dn_item = delivery_note.append("items", {})
						
						# Copy item fields from sales order item
						dn_item.item_code = so_item.item_code
						dn_item.item_name = so_item.item_name
						dn_item.description = so_item.description
						dn_item.qty = pending_qty
						dn_item.uom = so_item.uom
						dn_item.conversion_factor = so_item.conversion_factor
						dn_item.stock_qty = flt(pending_qty) * flt(so_item.conversion_factor)
						dn_item.rate = so_item.rate
						dn_item.amount = flt(pending_qty) * flt(so_item.rate)
						dn_item.base_rate = so_item.base_rate
						dn_item.base_amount = flt(pending_qty) * flt(so_item.base_rate)
						dn_item.warehouse = so_item.warehouse
						
						# Set target warehouse on item level for internal customers
						if sales_order.is_internal_customer:
							dn_item.target_warehouse = target_warehouse
						
						dn_item.against_sales_order = sales_order.name
						dn_item.so_detail = so_item.name
						dn_item.cost_center = so_item.cost_center
						dn_item.project = so_item.project
						
						# Copy other relevant fields
						if hasattr(so_item, 'item_group'):
							dn_item.item_group = so_item.item_group
						if hasattr(so_item, 'brand'):
							dn_item.brand = so_item.brand
				
				# Copy taxes from sales order if any
				for tax in sales_order.taxes:
					dn_tax = delivery_note.append("taxes", {})
					dn_tax.charge_type = tax.charge_type
					dn_tax.account_head = tax.account_head
					dn_tax.rate = tax.rate
					dn_tax.description = tax.description
					dn_tax.cost_center = tax.cost_center
				
				# Set missing values and calculate totals
				delivery_note.run_method("set_missing_values")
				delivery_note.run_method("calculate_taxes_and_totals")
				
				# Insert the delivery note (this keeps it in draft state)
				delivery_note.insert()
				
				created_delivery_notes.append({
					"sales_order_id": sales_order_id,
					"delivery_note": delivery_note.name,
					"customer": delivery_note.customer,
					"customer_name": delivery_note.customer_name,
					"total_qty": delivery_note.total_qty,
					"grand_total": delivery_note.grand_total,
					"driver": delivery_note.driver,
					"vehicle_no": delivery_note.vehicle_no,
					"posting_date": delivery_note.posting_date,
					"posting_time": delivery_note.posting_time
				})
				
			except Exception as e:
				frappe.log_error(
					title="Delivery Note Creation Failed for Sales Order",
					message=f"Sales Order: {sales_order_id}\nError: {str(e)}\nTraceback: {frappe.get_traceback()}"
				)
				failed_sales_orders.append({
					"sales_order_id": sales_order_id,
					"error": str(e)
				})
		
		# Commit transaction
		frappe.db.commit()
		
		frappe.local.response['http_status_code'] = 201
		return {
			"success": True,
			"status": "success",
			"message": f"Created {len(created_delivery_notes)} delivery notes successfully",
			"data": {
				"created_delivery_notes": created_delivery_notes,
				"failed_sales_orders": failed_sales_orders,
				"total_processed": len(sales_order_ids),
				"total_created": len(created_delivery_notes),
				"total_failed": len(failed_sales_orders)
			},
			"http_status_code": 201
		}
		
	except Exception as e:
		frappe.db.rollback()
		frappe.log_error(
			title="Start Sales Order Delivery Failed",
			message=f"Employee: {employee_id if 'employee_id' in locals() else 'Unknown'}\nError: {str(e)}\nTraceback: {frappe.get_traceback()}"
		)
		
		frappe.local.response['http_status_code'] = 500
		return {
			"success": False,
			"status": "error",
			"message": str(e),
			"code": "START_DELIVERY_FAILED",
			"http_status_code": 500
		}

@frappe.whitelist(allow_guest=True, methods=["POST"])
def create_multiple_delivery_notes_from_sales_orders():
	"""
	Create multiple delivery notes from sales orders with aggregated items and signature
	
	Required header: Authorization Bearer token
	Required body params:
		sales_order_ids: list - Array of Sales Order IDs
			[
				"SO-001",
				"SO-002"
			]
		signature: str - Base64 encoded signature image (optional, used for all delivery notes)
		items: list - Array of aggregated items with item_code and qty
			[
				{
					"item_code": "ITEM001",
					"qty": 50  # Total available quantity for this item
				}
			]
	
	Returns:
		dict: Success/error response with delivery note details for all created notes
	"""
	# Start transaction
	frappe.db.begin()
	
	try:
		# Verify authorization
		is_valid, result = verify_dp_token(frappe.request.headers)
		if not is_valid:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 401
			return result
		
		employee_id = result["employee"]
		
		# Get request data
		data = frappe.request.get_json()
		
		if not data:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "No data provided",
				"code": "NO_DATA",
				"http_status_code": 400
			}
		
		sales_order_ids = data.get("sales_order_ids", [])
		signature_base64 = data.get("signature")
		aggregated_items = data.get("items", [])
		
		if not sales_order_ids:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "Sales Order IDs are required",
				"code": "MISSING_SALES_ORDER_IDS",
				"http_status_code": 400
			}
		
		if not isinstance(sales_order_ids, list):
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "sales_order_ids must be a list",
				"code": "INVALID_SALES_ORDER_IDS_FORMAT",
				"http_status_code": 400
			}
		
		if not aggregated_items:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "Items are required",
				"code": "MISSING_ITEMS",
				"http_status_code": 400
			}
		
		# Clean and validate sales order IDs
		sales_order_ids = [so_id.strip() for so_id in sales_order_ids if so_id and so_id.strip()]
		
		if not sales_order_ids:
			frappe.db.rollback()
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "No valid sales order IDs found",
				"code": "NO_VALID_SALES_ORDER_IDS",
				"http_status_code": 400
			}
		
		# Get driver record for the authenticated employee
		driver_record = frappe.db.get_value(
			"Driver",
			{"employee": employee_id, "status": "Active"},
			"name"
		)
		
		vehicle_no = None
		if driver_record:
			# Get first vehicle assigned to the driver
			vehicle = frappe.db.sql("""
				SELECT name, license_plate
				FROM `tabVehicle`
				WHERE custom_driver = %(driver)s
				ORDER BY creation ASC
				LIMIT 1
			""", {"driver": driver_record}, as_dict=True)
			
			if vehicle:
				vehicle_no = vehicle[0].license_plate or vehicle[0].name
		
		# Create aggregated items map for easy lookup
		available_items = {}
		for item in aggregated_items:
			item_code = item.get("item_code")
			qty = flt(item.get("qty", 0))
			
			if not item_code:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": "Item code is required for all items",
					"code": "MISSING_ITEM_CODE",
					"http_status_code": 400
				}
			
			if qty <= 0:
				frappe.db.rollback()
				frappe.local.response['http_status_code'] = 400
				return {
					"success": False,
					"status": "error",
					"message": f"Quantity must be greater than 0 for item {item_code}",
					"code": "INVALID_QUANTITY",
					"http_status_code": 400
				}
			
			available_items[item_code] = qty
		
		# Process each sales order
		created_delivery_notes = []
		failed_sales_orders = []
		not_found_orders = []
		not_submitted_orders = []
		insufficient_quantity_orders = []
		
		for sales_order_id in sales_order_ids:
			try:
				# Check if sales order exists
				if not frappe.db.exists("Sales Order", sales_order_id):
					not_found_orders.append(sales_order_id)
					continue
				
				# Get sales order
				sales_order = frappe.get_doc("Sales Order", sales_order_id)
				
				if sales_order.docstatus != 1:
					not_submitted_orders.append(sales_order_id)
					continue
				
				# Check if delivery note already exists for this sales order with same driver and vehicle
				existing_dn_ids = frappe.db.sql("""
					SELECT DISTINCT parent 
					FROM `tabDelivery Note Item` 
					WHERE against_sales_order = %s
				""", sales_order_id, as_list=True)
				
				existing_dn = None
				if existing_dn_ids:
					# Get the first delivery note that matches our criteria
					for dn_id in existing_dn_ids:
						dn_name = dn_id[0]
						dn_doc = frappe.get_doc("Delivery Note", dn_name)
						
						# Check if existing DN has same driver and vehicle
						if (dn_doc.driver == driver_record and 
							dn_doc.vehicle_no == vehicle_no):
							existing_dn = dn_name
							break
					
					# If no matching driver/vehicle found, check if any non-cancelled DN exists
					if not existing_dn:
						for dn_id in existing_dn_ids:
							dn_name = dn_id[0]
							dn_doc = frappe.get_doc("Delivery Note", dn_name)
							if dn_doc.docstatus != 2:  # Not cancelled
								existing_dn = dn_name
								break
				
				if existing_dn:
					# Get the existing delivery note to check driver and vehicle
					existing_dn_doc = frappe.get_doc("Delivery Note", existing_dn)
					
					# Check if existing DN has same driver and vehicle
					if (existing_dn_doc.driver == driver_record and 
						existing_dn_doc.vehicle_no == vehicle_no):
						# Skip creation if same driver and vehicle
						failed_sales_orders.append({
							"sales_order_id": sales_order_id,
							"reason": f"Delivery Note already exists with same driver and vehicle: {existing_dn}"
						})
						continue
					else:
						# Different driver/vehicle, check if we can create new one
						if existing_dn_doc.docstatus != 2:  # Not cancelled
							failed_sales_orders.append({
								"sales_order_id": sales_order_id,
								"reason": f"Delivery Note already exists and is not cancelled: {existing_dn}"
							})
							continue
				
				# Check target warehouse requirement for internal customers
				target_warehouse = None
				if sales_order.is_internal_customer:
					target_warehouse = sales_order.get("custom_set_target_warehouse")
					if not target_warehouse:
						# Try to get from company default warehouse
						target_warehouse = frappe.db.get_value(
							"Warehouse", 
							{"is_group": 0, "company": sales_order.company}, 
							"name"
						)
					
					if not target_warehouse:
						failed_sales_orders.append({
							"sales_order_id": sales_order_id,
							"reason": "Target Warehouse is mandatory for internal transfers but not set"
						})
						continue
				
				# Validate and prepare items for this sales order
				so_items_map = {item.item_code: item for item in sales_order.items}
				delivery_items = []
				has_insufficient_quantity = False
				
				for item_code, available_qty in available_items.items():
					if item_code not in so_items_map:
						continue
					
					so_item = so_items_map[item_code]
					pending_qty = flt(so_item.qty) - flt(so_item.delivered_qty)
					
					if pending_qty <= 0:
						continue
					
					# Calculate how much we can deliver
					deliverable_qty = min(available_qty, pending_qty)
					
					if deliverable_qty > 0:
						delivery_items.append({
							"item_code": item_code,
							"qty": deliverable_qty,
							"so_item": so_item
						})
					else:
						has_insufficient_quantity = True
				
				if not delivery_items:
					if has_insufficient_quantity:
						insufficient_quantity_orders.append(sales_order_id)
					else:
						failed_sales_orders.append({
							"sales_order_id": sales_order_id,
							"reason": "No items available for delivery"
						})
					continue
				
				# Create delivery note
				delivery_note = frappe.new_doc("Delivery Note")
				
				# Copy header fields from sales order
				delivery_note.customer = sales_order.customer
				delivery_note.customer_name = sales_order.customer_name
				delivery_note.customer_address = sales_order.customer_address
				delivery_note.address_display = sales_order.address_display
				delivery_note.contact_person = sales_order.contact_person
				delivery_note.contact_display = sales_order.contact_display
				delivery_note.contact_email = sales_order.contact_email
				delivery_note.contact_mobile = sales_order.contact_mobile
				delivery_note.currency = sales_order.currency
				delivery_note.conversion_rate = sales_order.conversion_rate
				delivery_note.selling_price_list = sales_order.selling_price_list
				delivery_note.price_list_currency = sales_order.price_list_currency
				delivery_note.plc_conversion_rate = sales_order.plc_conversion_rate
				delivery_note.ignore_pricing_rule = sales_order.ignore_pricing_rule
				delivery_note.company = sales_order.company
				delivery_note.project = sales_order.project
				delivery_note.cost_center = sales_order.cost_center
				delivery_note.territory = sales_order.territory
				delivery_note.customer_group = sales_order.customer_group
				delivery_note.is_internal_customer = sales_order.is_internal_customer
				delivery_note.represents_company = sales_order.represents_company
				delivery_note.set_warehouse = sales_order.set_warehouse
				
				# Set driver and vehicle if found
				if driver_record:
					delivery_note.driver = driver_record
				if vehicle_no:
					delivery_note.vehicle_no = vehicle_no
				
				# Handle internal customer warehouse settings
				if sales_order.is_internal_customer and target_warehouse:
					delivery_note.set_target_warehouse = target_warehouse
				
				# Add items to delivery note
				for delivery_item in delivery_items:
					so_item = delivery_item["so_item"]
					dn_item = delivery_note.append("items", {})
					
					# Copy item fields from sales order item
					dn_item.item_code = so_item.item_code
					dn_item.item_name = so_item.item_name
					dn_item.description = so_item.description
					dn_item.qty = delivery_item["qty"]
					dn_item.uom = so_item.uom
					dn_item.conversion_factor = so_item.conversion_factor
					dn_item.stock_qty = flt(delivery_item["qty"]) * flt(so_item.conversion_factor)
					dn_item.rate = so_item.rate
					dn_item.amount = flt(delivery_item["qty"]) * flt(so_item.rate)
					dn_item.base_rate = so_item.base_rate
					dn_item.base_amount = flt(delivery_item["qty"]) * flt(so_item.base_rate)
					dn_item.warehouse = so_item.warehouse
					
					# Set target warehouse on item level for internal customers
					if sales_order.is_internal_customer and target_warehouse:
						dn_item.target_warehouse = target_warehouse
					
					dn_item.against_sales_order = sales_order.name
					dn_item.so_detail = so_item.name
					dn_item.cost_center = so_item.cost_center
					dn_item.project = so_item.project
					
					# Copy other relevant fields
					if hasattr(so_item, 'item_group'):
						dn_item.item_group = so_item.item_group
					if hasattr(so_item, 'brand'):
						dn_item.brand = so_item.brand
				
				# Copy taxes from sales order if any
				for tax in sales_order.taxes:
					dn_tax = delivery_note.append("taxes", {})
					dn_tax.charge_type = tax.charge_type
					dn_tax.account_head = tax.account_head
					dn_tax.rate = tax.rate
					dn_tax.description = tax.description
					dn_tax.cost_center = tax.cost_center
				
				# Set missing values and calculate totals
				delivery_note.run_method("set_missing_values")
				delivery_note.run_method("calculate_taxes_and_totals")
				
				# Insert the delivery note
				delivery_note.insert()
				
				# Update available quantities
				for delivery_item in delivery_items:
					item_code = delivery_item["item_code"]
					used_qty = delivery_item["qty"]
					available_items[item_code] -= used_qty
				
				# Add to created delivery notes
				created_delivery_notes.append({
					"delivery_note": delivery_note.name,
					"sales_order": sales_order_id,
					"customer": delivery_note.customer,
					"customer_name": delivery_note.customer_name,
					"total_qty": delivery_note.total_qty,
					"grand_total": delivery_note.grand_total,
					"driver": delivery_note.driver,
					"vehicle_no": delivery_note.vehicle_no,
					"posting_date": delivery_note.posting_date,
					"posting_time": delivery_note.posting_time
				})
				
			except Exception as e:
				frappe.log_error(
					title="Multiple Delivery Note Creation - Sales Order Processing Failed",
					message=f"Sales Order: {sales_order_id}\nError: {str(e)}\nTraceback: {frappe.get_traceback()}"
				)
				failed_sales_orders.append({
					"sales_order_id": sales_order_id,
					"reason": str(e)
				})
		
		# Handle signature if provided (create one file and attach to all delivery notes)
		signature_file_url = None
		if signature_base64 and created_delivery_notes:
			try:
				# Remove data URL prefix if present
				if signature_base64.startswith('data:'):
					signature_base64 = signature_base64.split('base64,')[1].strip()
				
				# Decode base64 string
				file_content = base64.b64decode(signature_base64)
				
				# Generate filename with timestamp
				filename = f"signature_multiple_dn_{frappe.utils.now_datetime().strftime('%Y%m%d%H%M%S')}.png"
				
				# Create file doc
				file_doc = frappe.get_doc({
					"doctype": "File",
					"file_name": filename,
					"content": file_content,
					"is_private": 0
				})
				file_doc.insert()
				signature_file_url = file_doc.file_url
				
				# Attach to all created delivery notes
				for dn_info in created_delivery_notes:
					file_doc.attached_to_doctype = "Delivery Note"
					file_doc.attached_to_name = dn_info["delivery_note"]
					file_doc.save()
				
			except Exception as e:
				frappe.log_error(
					title="Multiple Delivery Note Creation - Signature Processing Failed",
					message=f"Error: {str(e)}"
				)
				# Don't fail the entire operation for signature issues
		
		# Commit transaction
		frappe.db.commit()
		
		# Prepare response
		response_data = {
			"created_delivery_notes": created_delivery_notes,
			"signature_file_url": signature_file_url,
			"summary": {
				"total_requested": len(sales_order_ids),
				"created": len(created_delivery_notes),
				"failed": len(failed_sales_orders),
				"not_found": len(not_found_orders),
				"not_submitted": len(not_submitted_orders),
				"insufficient_quantity": len(insufficient_quantity_orders)
			}
		}
		
		# Add error details if any
		if failed_sales_orders:
			response_data["failed_sales_orders"] = failed_sales_orders
		if not_found_orders:
			response_data["not_found_orders"] = not_found_orders
		if not_submitted_orders:
			response_data["not_submitted_orders"] = not_submitted_orders
		if insufficient_quantity_orders:
			response_data["insufficient_quantity_orders"] = insufficient_quantity_orders
		
		# Determine response status
		if len(created_delivery_notes) == 0:
			frappe.local.response['http_status_code'] = 400
			return {
				"success": False,
				"status": "error",
				"message": "No delivery notes were created",
				"code": "NO_DELIVERY_NOTES_CREATED",
				"data": response_data,
				"http_status_code": 400
			}
		elif len(created_delivery_notes) < len(sales_order_ids):
			# Partial success
			frappe.local.response['http_status_code'] = 207  # Multi-Status
			return {
				"success": True,
				"status": "partial_success",
				"message": f"Created {len(created_delivery_notes)} out of {len(sales_order_ids)} delivery notes",
				"code": "PARTIAL_SUCCESS",
				"data": response_data,
				"http_status_code": 207
			}
		else:
			# Complete success
			frappe.local.response['http_status_code'] = 201
			return {
				"success": True,
				"status": "success",
				"message": f"Successfully created all {len(created_delivery_notes)} delivery notes",
				"code": "SUCCESS",
				"data": response_data,
				"http_status_code": 201
			}
		
	except Exception as e:
		frappe.db.rollback()
		frappe.log_error(
			title="Multiple Delivery Note Creation Failed",
			message=f"Employee: {employee_id if 'employee_id' in locals() else 'Unknown'}\nError: {str(e)}\nTraceback: {frappe.get_traceback()}"
		)
		
		frappe.local.response['http_status_code'] = 500
		return {
			"success": False,
			"status": "error",
			"message": str(e),
			"code": "MULTIPLE_DELIVERY_NOTE_CREATION_FAILED",
			"http_status_code": 500
		}
