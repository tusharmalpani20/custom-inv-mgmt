import frappe
from typing import Dict, Any

# Hardcoded API token for app authentication
API_TOKEN = "SID_FARM_CRATE_SCAN_9X72_TOKEN"  # This should be same in the mobile app

@frappe.whitelist(allow_guest=True, methods=["POST"])
def update_crate_scan(crate_id: str) -> Dict[str, Any]:
    """
    Updates the crate's printing_done_on and last_scanned_on fields when scanned via mobile app
    Required header: API-TOKEN
    Args:
        crate_id: ID of the crate being scanned
    """
    try:
        # Verify API token
        api_token = frappe.request.headers.get('API-TOKEN')
        if not api_token or api_token != API_TOKEN:
            frappe.local.response['http_status_code'] = 401
            return {
                "success": False,
                "status": "error",
                "message": "Invalid API token",
                "code": "INVALID_API_TOKEN",
                "http_status_code": 401
            }

        # Get crate document
        try:
            crate = frappe.get_doc("Crate", crate_id)
        except frappe.DoesNotExistError:
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": "Crate not found",
                "code": "CRATE_NOT_FOUND",
                "http_status_code": 404
            }

        # Check if crate has already been scanned
        if crate.printing_done_on and crate.last_scanned_on:
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "Crate has already been scanned",
                "code": "CRATE_ALREADY_SCANNED",
                "http_status_code": 400
            }

        # Update the scan fields
        current_date = frappe.utils.today()
        crate.db_set('printing_done_on', current_date, update_modified=True)
        crate.db_set('last_scanned_on', current_date, update_modified=True)

        frappe.db.commit()

        frappe.local.response['http_status_code'] = 200
        return {
            "success": True,
            "status": "success",
            "message": "Crate scan updated successfully",
            "code": "CRATE_SCAN_UPDATED",
            "data": {
                "crate_id": crate_id,
                "scan_date": current_date
            },
            "http_status_code": 200
        }

    except Exception as e:
        frappe.log_error(
            title="Update Crate Scan - Error",
            message={
                "error": str(e),
                "crate_id": crate_id,
                "traceback": frappe.get_traceback()
            }
        )
        frappe.db.rollback()
        frappe.local.response['http_status_code'] = 500
        return {
            "success": False,
            "status": "error",
            "message": f"Error updating crate scan: {str(e)}",
            "code": "INTERNAL_SERVER_ERROR",
            "http_status_code": 500
        }
