import frappe
from frappe import _
from datetime import datetime
import base64
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token, handle_error_response
from typing import Dict, Any

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
