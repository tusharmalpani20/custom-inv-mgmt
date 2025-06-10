import frappe
from frappe import _
from datetime import datetime
import base64
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token, handle_error_response
from typing import Dict, Any

@frappe.whitelist(allow_guest=True, methods=["POST"])
def acknowledge_delivery_note() -> Dict[str, Any]:
    """
    Updates delivery note with receiver's signature and acknowledgment time, then submits it
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
            delivery_note.db_set('custom_receiver_signature', file_doc.file_url)
            delivery_note.db_set('custom_receiving_time', current_time)
            
            # Submit the delivery note
            delivery_note.submit()
            
            # Commit transaction
            frappe.db.commit()
            
            frappe.local.response['http_status_code'] = 200
            return {
                "success": True,
                "status": "success",
                "message": "Delivery note acknowledged and submitted successfully",
                "code": "ACKNOWLEDGMENT_SAVED",
                "data": {
                    "delivery_note": delivery_note_id,
                    "signature_url": file_doc.file_url,
                    "receiving_time": str(current_time)
                },
                "http_status_code": 200
            }

        except Exception as e:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 500
            return {
                "success": False,
                "status": "error",
                "message": f"Failed to submit delivery note: {str(e)}",
                "code": "SUBMISSION_FAILED",
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
