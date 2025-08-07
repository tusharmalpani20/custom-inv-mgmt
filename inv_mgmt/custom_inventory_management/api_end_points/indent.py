import frappe
from frappe.model.document import Document
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token, handle_error_response


@frappe.whitelist(allow_guest=True)
def get_driver_delivery_route() -> Dict[str, Any]:
    """
    Get delivery route and delivery notes for the logged-in driver for today
    Required header: Auth-Token
    Returns:
        Dict containing delivery route info and delivery notes
    """
    try:
        # Verify token and authenticate
        is_valid, result = verify_dp_token(frappe.request.headers)
        if not is_valid:
            frappe.local.response['http_status_code'] = 401
            return result
        
        employee = result["employee"]
        today = frappe.utils.today()
        
        # Get today's indent for the driver
        indent = frappe.get_value(
            "SF Indent Master",
            {
                "driver": employee,
                "date": today,
                "docstatus": 1  # Only get submitted indents
            },
            ["name", "delivery_route", "vehicle", "vehicle_license_plate", "docstatus", "workflow_state"],
            as_dict=True
        )
        
        if not indent:
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": "No indent found for today",
                "code": "NO_INDENT",
                "http_status_code": 404
            }

        # Check if indent is in correct workflow state
        # "Approved By Plant"
        if indent.workflow_state not in ["Delivery Started"]:
            frappe.local.response['http_status_code'] = 200
            return {
                "success": True,
                "status": "success",
                # "message": "Delivery route details are only available after plant approval",
                # "code": "INVALID_WORKFLOW_STATE",
                "message": "Data fetched successfully",
                "code": "SUCCESS",
                "data": {
                    "indent": indent
                },
                "http_status_code": 200
            }
        
        # Get delivery route info
        delivery_route = frappe.get_doc("SF Delivery Route Master", indent.delivery_route)
        
        # Get start point warehouse details including lat/long
        start_point_warehouse = frappe.get_doc("Warehouse", delivery_route.start_point)
        
        # Get delivery notes for each customer in the route
        delivery_points = []
        for point in delivery_route.delivery_points:
            # Get delivery notes for this customer with shipping address details
            delivery_notes = frappe.get_all(
                "Delivery Note",
                filters={
                    "customer": point.customer,
                    "posting_date": today
                },
                fields=[
                    "name", "customer", "customer_name", "posting_date", 
                    "total_qty", "grand_total", "status",
                    "shipping_address_name", "shipping_address", 
                    "custom_shipping_address_latitude", "custom_shipping_address_longitude",
                    "contact_display", "contact_mobile", "docstatus", "workflow_state"
                ]
            )
            
            # Get address details if address is specified
            address_details = None
            if point.address:
                address_doc = frappe.get_doc("Address", point.address)
                address_details = {
                    "name": address_doc.name,
                    "address_type": address_doc.address_type,
                    "address_line1": address_doc.address_line1,
                    "address_line2": address_doc.address_line2,
                    "city": address_doc.city,
                    "state": address_doc.state,
                    "pincode": address_doc.pincode,
                    "country": address_doc.country,
                    "latitude": address_doc.custom_latitude,
                    "longitude": address_doc.custom_longitude
                }
            
            delivery_points.append({
                "customer": point.customer,
                "address": address_details,
                "customer_category": point.customer_category,
                "delivery_notes": [
                    {
                        **note,
                        "shipping_address_details": {
                            "name": note.shipping_address_name,
                            "address": note.shipping_address,
                            "latitude": note.custom_shipping_address_latitude,
                            "longitude": note.custom_shipping_address_longitude
                        } if note.shipping_address_name else None
                    } for note in delivery_notes
                ]
            })
        
        return {
            "success": True,
            "status": "success",
            "message": "Data fetched successfully",
            "code": "SUCCESS",
            "data": {
                "indent": indent,
                "delivery_route": {
                    "name": delivery_route.name,
                    "route_name": delivery_route.route_name,
                    "route_category": delivery_route.route_category,
                    "start_point": {
                        "warehouse": delivery_route.start_point,
                        "latitude": start_point_warehouse.custom_latitude,
                        "longitude": start_point_warehouse.custom_longitude
                    },
                    "branch": delivery_route.branch,
                    "delivery_points": delivery_points
                }
            },
            "http_status_code": 200
        }
        
    except Exception as e:
        frappe.log_error(str(e), "Driver Delivery Route API Error")
        frappe.local.response['http_status_code'] = 500
        return {
            "success": False,
            "status": "error",
            "message": "Failed to fetch delivery route data",
            "code": "SYSTEM_ERROR",
            "error": str(e),
            "http_status_code": 500
        }

@frappe.whitelist(allow_guest=True, methods=["PUT"])
def start_indent_delivery() -> Dict[str, Any]:
    """
    Starts an indent delivery by updating its workflow state
    Required header: Authorization Bearer token
    Required body params:
        indent: str - The indent ID
    Returns:
        Dict containing success/error information
    """
    # Start transaction
    frappe.db.begin()
    
    try:
        # Log incoming request
        frappe.log_error(
            title="Indent Delivery Start - Request",
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
        
        indent_id = data.get('indent')
        
        if not indent_id:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "indent is a required field",
                "code": "MISSING_FIELDS",
                "http_status_code": 400
            }
        
        # Get indent document
        try:
            indent = frappe.get_doc("SF Indent Master", indent_id)
        except frappe.DoesNotExistError:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": f"Indent {indent_id} not found",
                "code": "INDENT_NOT_FOUND",
                "http_status_code": 404
            }

        # Check if already submitted
        if indent.docstatus != 1:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": f"Indent {indent_id} must be submitted",
                "code": "NOT_SUBMITTED",
                "http_status_code": 400
            }

        # Verify workflow state is Approved By Plant
        if indent.workflow_state != "Approved By Plant":
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": f"Indent {indent_id} must be in Approved By Plant state to start delivery",
                "code": "INVALID_WORKFLOW_STATE",
                "http_status_code": 400
            }

        try:
            # Update indent using workflow
            current_time = frappe.utils.now_datetime()
            
            # Apply workflow action
            frappe.model.workflow.apply_workflow(indent, "Start Delivery")
            
            # Update trip start details
            indent.db_set('trip_started_at', current_time)
            indent.db_set('trip_started_by', employee)
            
            # Commit transaction
            frappe.db.commit()
            
            # Get delivery route info for response
            delivery_route = frappe.get_doc("SF Delivery Route Master", indent.delivery_route)
            start_point_warehouse = frappe.get_doc("Warehouse", delivery_route.start_point)
            
            # Get delivery notes for each customer in the route
            delivery_points = []
            for point in delivery_route.delivery_points:
                # Get delivery notes for this customer with shipping address details
                delivery_notes = frappe.get_all(
                    "Delivery Note",
                    filters={
                        "customer": point.customer,
                        "posting_date": indent.date
                    },
                    fields=[
                        "name", "customer", "customer_name", "posting_date", 
                        "total_qty", "grand_total", "status",
                        "shipping_address_name", "shipping_address", 
                        "custom_shipping_address_latitude", "custom_shipping_address_longitude",
                        "contact_display", "contact_mobile", "docstatus", "workflow_state"
                    ]
                )
                
                # Get address details if address is specified
                address_details = None
                if point.address:
                    address_doc = frappe.get_doc("Address", point.address)
                    address_details = {
                        "name": address_doc.name,
                        "address_type": address_doc.address_type,
                        "address_line1": address_doc.address_line1,
                        "address_line2": address_doc.address_line2,
                        "city": address_doc.city,
                        "state": address_doc.state,
                        "pincode": address_doc.pincode,
                        "country": address_doc.country,
                        "latitude": address_doc.custom_latitude,
                        "longitude": address_doc.custom_longitude
                    }
                
                delivery_points.append({
                    "customer": point.customer,
                    "address": address_details,
                    "customer_category": point.customer_category,
                    "delivery_notes": [
                        {
                            **note,
                            "shipping_address_details": {
                                "name": note.shipping_address_name,
                                "address": note.shipping_address,
                                "latitude": note.custom_shipping_address_latitude,
                                "longitude": note.custom_shipping_address_longitude
                            } if note.shipping_address_name else None
                        } for note in delivery_notes
                    ]
                })
            
            frappe.local.response['http_status_code'] = 200
            return {
                "success": True,
                "status": "success",
                "message": "Indent delivery started successfully",
                "code": "DELIVERY_STARTED",
                "data": {
                    "indent": {
                        "name": indent_id,
                        "delivery_route": indent.delivery_route,
                        "vehicle": indent.vehicle,
                        "vehicle_license_plate": indent.vehicle_license_plate,
                        "workflow_state": indent.workflow_state,
                        "trip_started_at": str(current_time),
                        "trip_started_by": employee
                    },
                    "delivery_route": {
                        "name": delivery_route.name,
                        "route_name": delivery_route.route_name,
                        "route_category": delivery_route.route_category,
                        "start_point": {
                            "warehouse": delivery_route.start_point,
                            "latitude": start_point_warehouse.custom_latitude,
                            "longitude": start_point_warehouse.custom_longitude
                        },
                        "branch": delivery_route.branch,
                        "delivery_points": delivery_points
                    }
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
                "message": f"Failed to start indent delivery: {str(e)}",
                "code": "START_FAILED",
                "http_status_code": 500
            }

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(
            title="Indent Delivery Start Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        frappe.local.response['http_status_code'] = 500
        return handle_error_response(e, "Error processing indent delivery start")