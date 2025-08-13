import frappe
from frappe.model.document import Document
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token, handle_error_response
from collections import defaultdict


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


@frappe.whitelist(allow_guest=True, methods=["PUT"])
def start_multiple_indent_deliveries() -> Dict[str, Any]:
    """
    Starts multiple indent deliveries by updating their workflow states
    Required header: Authorization Bearer token
    Required body params:
        indents: List[str] - Array of indent IDs
    Returns:
        Dict containing success/error information for each indent
    """
    # Start transaction
    frappe.db.begin()
    
    try:
        # Log incoming request
        frappe.log_error(
            title="Multiple Indent Delivery Start - Request",
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
        
        indent_ids = data.get('indents')
        
        if not indent_ids or not isinstance(indent_ids, list):
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "indents field must be a non-empty array",
                "code": "MISSING_FIELDS",
                "http_status_code": 400
            }
        
        if len(indent_ids) == 0:
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "indents array cannot be empty",
                "code": "EMPTY_ARRAY",
                "http_status_code": 400
            }
        
        # Track results for each indent
        results = {
            "successful": [],
            "failed": [],
            "total_processed": len(indent_ids),
            "total_successful": 0,
            "total_failed": 0
        }
        
        # Process each indent individually
        for indent_id in indent_ids:
            try:
                # Get indent document
                try:
                    indent = frappe.get_doc("SF Indent Master", indent_id)
                except frappe.DoesNotExistError:
                    results["failed"].append({
                        "indent_id": indent_id,
                        "error": "Indent not found",
                        "code": "INDENT_NOT_FOUND"
                    })
                    results["total_failed"] += 1
                    continue

                # Check if already submitted
                if indent.docstatus != 1:
                    results["failed"].append({
                        "indent_id": indent_id,
                        "error": "Indent must be submitted",
                        "code": "NOT_SUBMITTED"
                    })
                    results["total_failed"] += 1
                    continue

                # Verify workflow state is Approved By Plant
                if indent.workflow_state != "Approved By Plant":
                    results["failed"].append({
                        "indent_id": indent_id,
                        "error": "Indent must be in Approved By Plant state to start delivery",
                        "code": "INVALID_WORKFLOW_STATE"
                    })
                    results["total_failed"] += 1
                    continue

                # Try to apply workflow action
                try:
                    current_time = frappe.utils.now_datetime()
                    
                    # Apply workflow action
                    frappe.model.workflow.apply_workflow(indent, "Start Delivery")
                    
                    # Update trip start details
                    indent.db_set('trip_started_at', current_time)
                    indent.db_set('trip_started_by', employee)
                    
                    # Add to successful results
                    results["successful"].append({
                        "indent_id": indent_id,
                        "workflow_state": indent.workflow_state,
                        "trip_started_at": str(current_time),
                        "trip_started_by": employee
                    })
                    results["total_successful"] += 1
                    
                except frappe.exceptions.WorkflowTransitionError as e:
                    results["failed"].append({
                        "indent_id": indent_id,
                        "error": str(e),
                        "code": "WORKFLOW_TRANSITION_ERROR"
                    })
                    results["total_failed"] += 1
                    continue
                    
            except Exception as e:
                results["failed"].append({
                    "indent_id": indent_id,
                    "error": f"Unexpected error: {str(e)}",
                    "code": "UNEXPECTED_ERROR"
                })
                results["total_failed"] += 1
                continue
        
        # Determine response based on results
        if results["total_successful"] == 0:
            # All indents failed - rollback and return error
            frappe.db.rollback()
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": "All indents failed to start delivery",
                "code": "ALL_FAILED",
                "data": results,
                "http_status_code": 400
            }
        
        elif results["total_failed"] == 0:
            # All indents succeeded - commit and return success
            frappe.db.commit()
            frappe.local.response['http_status_code'] = 200
            return {
                "success": True,
                "status": "success",
                "message": "All indents started delivery successfully",
                "code": "ALL_SUCCESS",
                "data": results,
                "http_status_code": 200
            }
        
        else:
            # Mixed results - commit successful ones and return partial success
            frappe.db.commit()
            frappe.local.response['http_status_code'] = 207  # Multi-Status
            return {
                "success": True,
                "status": "partial_success",
                "message": f"Started delivery for {results['total_successful']} indents, {results['total_failed']} failed",
                "code": "PARTIAL_SUCCESS",
                "data": results,
                "http_status_code": 207
            }

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(
            title="Multiple Indent Delivery Start Error",
            message=f"Error: {str(e)}\nTraceback: {frappe.get_traceback()}"
        )
        frappe.local.response['http_status_code'] = 500
        return handle_error_response(e, "Error processing multiple indent delivery starts")


@frappe.whitelist()
def create_adjusted_indents_for_shortfall() -> Dict[str, Any]:
    """
    Create adjusted indents for all routes where sales orders exceed indent quantities.
    This function checks all submitted indents for today's date and creates adjusted indents
    when there's a shortfall in quantity.
    
    Returns:
        Dict containing processing results and created adjusted indents
    """
    try:
        today = "2025-07-01" #frappe.utils.today()
        print(f"=== Starting adjusted indent creation process for date: {today} ===")
        frappe.logger().info(f"Starting adjusted indent creation process for date: {today}")
        
        # Get all submitted indents for today
        indents = frappe.get_all(
            "SF Indent Master",
            filters={
                "date": today,
                "docstatus": 1  # Only submitted indents
            },
            fields=["name", "delivery_route", "for", "company", "date"]
        )
        
        print(f"Found {len(indents)} submitted indents for today")
        
        if not indents:
            print("No submitted indents found for today")
            return {
                "success": True,
                "status": "success",
                "message": "No submitted indents found for today",
                "data": {
                    "processed_indents": 0,
                    "created_adjusted_indents": 0,
                    "indents_with_shortfall": 0,
                    "indents_without_shortfall": 0,
                    "already_adjusted_indents": 0,
                    "details": []
                }
            }
        
        # Track processing results
        results = {
            "processed_indents": 0,
            "created_adjusted_indents": 0,
            "indents_with_shortfall": 0,
            "indents_without_shortfall": 0,
            "already_adjusted_indents": 0,
            "details": []
        }
        
        # Process each indent
        for indent_data in indents:
            print(f"\n--- Processing indent: {indent_data['name']} ---")
            try:
                indent_result = process_indent_for_shortfall(indent_data, today)
                results["processed_indents"] += 1
                results["details"].append(indent_result)
                
                print(f"Indent {indent_data['name']} result: {indent_result['status']}")
                
                if indent_result["status"] == "adjusted_indent_created":
                    results["created_adjusted_indents"] += 1
                    results["indents_with_shortfall"] += 1
                elif indent_result["status"] == "shortfall_but_already_adjusted":
                    results["already_adjusted_indents"] += 1
                    results["indents_with_shortfall"] += 1
                elif indent_result["status"] == "no_shortfall":
                    results["indents_without_shortfall"] += 1
                
            except Exception as e:
                print(f"ERROR processing indent {indent_data['name']}: {str(e)}")
                print(f"Error type: {type(e)}")
                print(f"Error traceback: {frappe.get_traceback()}")
                frappe.logger().error(f"Error processing indent {indent_data['name']}: {str(e)}")
                results["details"].append({
                    "indent_name": indent_data["name"],
                    "status": "error",
                    "message": f"Error processing indent: {str(e)}"
                })
                results["processed_indents"] += 1
        
        print(f"\n=== Final Results ===")
        print(f"Processed: {results['processed_indents']}")
        print(f"Created adjusted indents: {results['created_adjusted_indents']}")
        print(f"With shortfall: {results['indents_with_shortfall']}")
        print(f"Without shortfall: {results['indents_without_shortfall']}")
        print(f"Already adjusted: {results['already_adjusted_indents']}")
        
        return {
            "success": True,
            "status": "success",
            "message": f"Processed {results['processed_indents']} indents. Created {results['created_adjusted_indents']} adjusted indents.",
            "data": results
        }
        
    except Exception as e:
        print(f"CRITICAL ERROR in create_adjusted_indents_for_shortfall: {str(e)}")
        print(f"Error type: {type(e)}")
        print(f"Error traceback: {frappe.get_traceback()}")
        frappe.logger().error(f"Error in create_adjusted_indents_for_shortfall: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "message": f"Failed to process adjusted indents: {str(e)}"
        }


def process_indent_for_shortfall(indent_data: Dict, date: str) -> Dict[str, Any]:
    """
    Process a single indent to check for shortfall and create adjusted indent if needed
    
    Args:
        indent_data: Dict containing indent information
        date: Date string in YYYY-MM-DD format
    
    Returns:
        Dict containing processing result for this indent
    """
    indent_name = indent_data["name"]
    delivery_route = indent_data["delivery_route"]
    plant_warehouse = indent_data["for"]
    
    print(f"Processing indent: {indent_name}")
    print(f"Delivery route: {delivery_route}")
    print(f"Plant warehouse: {plant_warehouse}")
    
    try:
        # Check if adjusted indent already exists for this route
        print("Checking for existing adjusted indent...")
        existing_adjusted = frappe.get_value(
            "SF Indent Master",
            {
                "adjusted_indent_for": indent_name,
                "is_adjusted_indent": 1,
                "docstatus": ["!=", 2]  # Not cancelled
            },
            "name"
        )
        
        if existing_adjusted:
            print(f"Adjusted indent already exists: {existing_adjusted}")
            return {
                "indent_name": indent_name,
                "delivery_route": delivery_route,
                "status": "shortfall_but_already_adjusted",
                "message": f"Adjusted indent {existing_adjusted} already exists for this indent",
                "existing_adjusted_indent": existing_adjusted
            }
        
        # Get route and delivery points
        print("Getting route document...")
        route_doc = frappe.get_doc("SF Delivery Route Master", delivery_route)
        print(f"Route document: {route_doc.name}")
        print(f"Number of delivery points: {len(route_doc.delivery_points)}")
        
        # Get original indent items
        print("Getting original indent items...")
        original_indent_items = get_indent_items(indent_name)
        print(f"Original indent items: {original_indent_items}")
        
        # Get aggregated sales order quantities for all delivery points
        print("Getting aggregated sales orders...")
        aggregated_sales_orders = get_aggregated_sales_orders_for_route(route_doc, date)
        print(f"Aggregated sales orders: {aggregated_sales_orders}")
        
        # Calculate shortfall
        print("Calculating shortfall...")
        shortfall_items = calculate_shortfall(original_indent_items, aggregated_sales_orders)
        print(f"Shortfall items: {shortfall_items}")
        
        if not shortfall_items:
            print("No shortfall detected")
            return {
                "indent_name": indent_name,
                "delivery_route": delivery_route,
                "status": "no_shortfall",
                "message": "No shortfall detected for this indent"
            }
        
        # Create adjusted indent
        print("Creating adjusted indent...")
        adjusted_indent_name = create_adjusted_indent(
            original_indent=indent_data,
            shortfall_items=shortfall_items,
            route_doc=route_doc
        )
        
        print(f"Adjusted indent created: {adjusted_indent_name}")
        
        return {
            "indent_name": indent_name,
            "delivery_route": delivery_route,
            "status": "adjusted_indent_created",
            "message": f"Adjusted indent {adjusted_indent_name} created successfully",
            "adjusted_indent_name": adjusted_indent_name,
            "shortfall_items": shortfall_items
        }
        
    except Exception as e:
        print(f"ERROR in process_indent_for_shortfall for {indent_name}: {str(e)}")
        print(f"Error type: {type(e)}")
        print(f"Error traceback: {frappe.get_traceback()}")
        frappe.logger().error(f"Error processing indent {indent_name}: {str(e)}")
        return {
            "indent_name": indent_name,
            "delivery_route": delivery_route,
            "status": "error",
            "message": f"Error processing indent: {str(e)}"
        }


def get_indent_items(indent_name: str) -> Dict[str, float]:
    """
    Get items and quantities from an indent
    
    Args:
        indent_name: Name of the indent
    
    Returns:
        Dict with item_code -> quantity mapping
    """
    print(f"Getting indent items for: {indent_name}")
    
    items = frappe.get_all(
        "SF Indent Item",
        filters={"parent": indent_name},
        fields=["sku", "quantity"]
    )
    
    print(f"Found {len(items)} indent items")
    for item in items:
        print(f"  Item: {item['sku']}, Quantity: {item['quantity']}")
    
    result = {item["sku"]: item["quantity"] for item in items}
    print(f"Returning indent items: {result}")
    
    return result


def get_aggregated_sales_orders_for_route(route_doc: object, date: str) -> Dict[str, float]:
    """
    Get aggregated sales order quantities for all delivery points in a route
    
    Args:
        route_doc: SF Delivery Route Master document
        date: Date string in YYYY-MM-DD format
    
    Returns:
        Dict with item_code -> total_quantity mapping
    """
    print(f"Getting aggregated sales orders for route: {route_doc.name}")
    print(f"Date: {date}")
    
    aggregated_items = defaultdict(float)
    
    # Get internal customer for warehouse deliveries
    print("Getting internal customer...")
    internal_customer = frappe.get_value("Customer", {"is_internal_customer": 1}, "name")
    print(f"Internal customer: {internal_customer}")
    
    if not internal_customer:
        print("WARNING: No internal customer found, skipping warehouse deliveries")
        frappe.logger().warning("No internal customer found, skipping warehouse deliveries")
    
    # Process each delivery point
    print(f"Processing {len(route_doc.delivery_points)} delivery points...")
    for i, point in enumerate(route_doc.delivery_points):
        print(f"  Delivery point {i+1}: {point.drop_type} - {point.drop_point}")
        
        if point.drop_type == "Customer":
            print(f"    Getting sales orders for customer: {point.drop_point}")
            # Get sales orders for customer
            customer_sales_orders = get_sales_orders_for_customer_on_date(point.drop_point, date)
            print(f"    Found {len(customer_sales_orders)} sales orders for customer")
            aggregate_sales_order_items(customer_sales_orders, aggregated_items)
            
        elif point.drop_type == "Warehouse" and internal_customer:
            print(f"    Getting sales orders for warehouse: {point.drop_point}")
            # Get sales orders for warehouse (internal customer with shipping address)
            warehouse_sales_orders = get_sales_orders_for_warehouse_on_date(point.drop_point, internal_customer, date)
            print(f"    Found {len(warehouse_sales_orders)} sales orders for warehouse")
            aggregate_sales_order_items(warehouse_sales_orders, aggregated_items)
    
    result = dict(aggregated_items)
    print(f"Final aggregated sales orders: {result}")
    return result


def get_sales_orders_for_customer_on_date(customer_name: str, date: str) -> List[Dict]:
    """
    Get sales orders for a specific customer on a specific date
    
    Args:
        customer_name: Customer name
        date: Date string in YYYY-MM-DD format
    
    Returns:
        List of sales order dictionaries
    """
    print(f"Getting sales orders for customer {customer_name} on {date}")
    
    result = frappe.db.sql("""
        SELECT so.name, soi.item_code, soi.qty
        FROM `tabSales Order` so
        INNER JOIN `tabSales Order Item` soi ON so.name = soi.parent
        WHERE so.customer = %(customer)s
        AND so.transaction_date = %(date)s
        AND so.docstatus = 1
    """, {
        "customer": customer_name,
        "date": date
    }, as_dict=True)
    
    print(f"Found {len(result)} sales orders for customer {customer_name}")
    for order in result:
        print(f"  SO: {order['name']}, Item: {order['item_code']}, Qty: {order['qty']}")
    
    return result


def get_sales_orders_for_warehouse_on_date(warehouse_name: str, internal_customer: str, date: str) -> List[Dict]:
    """
    Get sales orders for a specific warehouse (internal customer) on a specific date
    
    Args:
        warehouse_name: Warehouse name
        internal_customer: Internal customer name
        date: Date string in YYYY-MM-DD format
    
    Returns:
        List of sales order dictionaries
    """
    print(f"Getting sales orders for warehouse {warehouse_name} on {date}")
    
    # Get shipping address from SF Facility Master
    print("Getting facility data...")
    facility_data = frappe.db.get_value(
        "SF Facility Master",
        {"warehouse": warehouse_name},
        "shipping_address"
    )
    print(f"Facility shipping address: {facility_data}")
    
    if not facility_data:
        print("No facility data found, returning empty list")
        return []
    
    result = frappe.db.sql("""
        SELECT so.name, soi.item_code, soi.qty
        FROM `tabSales Order` so
        INNER JOIN `tabSales Order Item` soi ON so.name = soi.parent
        WHERE so.customer = %(customer)s
        AND so.transaction_date = %(date)s
        AND so.shipping_address_name = %(shipping_address)s
        AND so.docstatus = 1
    """, {
        "customer": internal_customer,
        "date": date,
        "shipping_address": facility_data
    }, as_dict=True)
    
    print(f"Found {len(result)} sales orders for warehouse {warehouse_name}")
    for order in result:
        print(f"  SO: {order['name']}, Item: {order['item_code']}, Qty: {order['qty']}")
    
    return result


def aggregate_sales_order_items(sales_orders: List[Dict], aggregated_items: Dict[str, float]) -> None:
    """
    Aggregate items from sales orders into the aggregated_items dictionary
    
    Args:
        sales_orders: List of sales order dictionaries
        aggregated_items: Dictionary to aggregate items into
    """
    print(f"Aggregating {len(sales_orders)} sales orders")
    
    for order in sales_orders:
        item_code = order["item_code"]
        qty = order["qty"]
        print(f"  Adding {qty} of {item_code}")
        aggregated_items[item_code] += qty
    
    print(f"Current aggregated items: {dict(aggregated_items)}")


def calculate_shortfall(indent_items: Dict[str, float], sales_order_items: Dict[str, float]) -> Dict[str, float]:
    """
    Calculate shortfall by comparing indent quantities with sales order quantities
    
    Args:
        indent_items: Dict with item_code -> indent_quantity mapping
        sales_order_items: Dict with item_code -> sales_order_quantity mapping
    
    Returns:
        Dict with item_code -> shortfall_quantity mapping (only items with shortfall)
    """
    print("Calculating shortfall...")
    print(f"Indent items: {indent_items}")
    print(f"Sales order items: {sales_order_items}")
    
    shortfall = {}
    
    for item_code, sales_qty in sales_order_items.items():
        indent_qty = indent_items.get(item_code, 0)
        print(f"  Item: {item_code}, Indent Qty: {indent_qty}, Sales Qty: {sales_qty}")
        
        if sales_qty > indent_qty:
            shortfall_qty = sales_qty - indent_qty
            shortfall[item_code] = shortfall_qty
            print(f"    SHORTFALL: {shortfall_qty}")
    
    print(f"Final shortfall: {shortfall}")
    return shortfall


def create_adjusted_indent(original_indent: Dict, shortfall_items: Dict[str, float], route_doc: object) -> str:
    """
    Create an adjusted indent for the shortfall quantities
    
    Args:
        original_indent: Original indent data
        shortfall_items: Dict with item_code -> shortfall_quantity mapping
        route_doc: SF Delivery Route Master document
    
    Returns:
        Name of the created adjusted indent
    """
    print("Creating adjusted indent...")
    print(f"Original indent: {original_indent}")
    print(f"Shortfall items: {shortfall_items}")
    
    try:
        # Create new indent document
        print("Creating new SF Indent Master document...")
        adjusted_indent = frappe.new_doc("SF Indent Master")
        
        print("Setting basic fields...")
        adjusted_indent.delivery_route = original_indent["delivery_route"]
        adjusted_indent.set("for", original_indent["for"])  # Plant warehouse
        adjusted_indent.date = original_indent["date"]
        adjusted_indent.company = original_indent["company"]
        adjusted_indent.is_adjusted_indent = 1
        adjusted_indent.adjusted_indent_for = original_indent["name"]
        
        # IMPORTANT: Set workflow state to Draft explicitly for API creation
        adjusted_indent.workflow_state = "Draft"
        print(f"Set workflow state to: {adjusted_indent.workflow_state}")
        
        # NOTE: Vehicle details are NOT inherited - user needs to add them manually
        print("Vehicle details will need to be added manually by the user")
        
        print("Adding shortfall items...")
        # Add shortfall items
        for item_code, shortfall_qty in shortfall_items.items():
            print(f"  Adding item: {item_code}, qty: {shortfall_qty}")
            
            # Get item details to ensure we have UOM
            item_doc = frappe.get_doc("Item", item_code)
            print(f"    Item UOM: {item_doc.stock_uom}")
            
            adjusted_indent.append("items", {
                "sku": item_code,
                "quantity": shortfall_qty,
                "uom": item_doc.stock_uom  # Add UOM field
            })
        
        # Validate the document before inserting
        print("Validating adjusted indent...")
        adjusted_indent.validate()
        
        print("Inserting adjusted indent (keeping in draft state)...")
        # Insert the adjusted indent but don't submit - keep in draft state
        adjusted_indent.insert()
        
        print(f"Successfully created adjusted indent in draft state: {adjusted_indent.name}")
        print(f"Document status: {adjusted_indent.docstatus}")
        print(f"Workflow state: {adjusted_indent.workflow_state}")
        print("NOTE: User needs to add vehicle details manually before using 'Send To Plant' action")
        
        frappe.logger().info(f"Created adjusted indent {adjusted_indent.name} in draft state for original indent {original_indent['name']}")
        
        return adjusted_indent.name
        
    except Exception as e:
        print(f"ERROR creating adjusted indent: {str(e)}")
        print(f"Error type: {type(e)}")
        print(f"Error traceback: {frappe.get_traceback()}")
        raise e


@frappe.whitelist()
def test_create_adjusted_indents(date: str = None) -> Dict[str, Any]:
    """
    Test endpoint for creating adjusted indents for a specific date
    
    Args:
        date: Date string in YYYY-MM-DD format (defaults to today)
    
    Returns:
        Dict containing processing results and created adjusted indents
    """
    if not date:
        date = frappe.utils.today()
    
    print(f"=== TEST: Creating adjusted indents for date: {date} ===")
    
    try:
        frappe.logger().info(f"Testing adjusted indent creation for date: {date}")
        
        # Get all submitted indents for the specified date
        indents = frappe.get_all(
            "SF Indent Master",
            filters={
                "date": date,
                "docstatus": 1  # Only submitted indents
            },
            fields=["name", "delivery_route", "for", "company", "date"]
        )
        
        print(f"Found {len(indents)} submitted indents for date: {date}")
        
        if not indents:
            print("No submitted indents found for the specified date")
            return {
                "success": True,
                "status": "success",
                "message": f"No submitted indents found for date: {date}",
                "data": {
                    "test_date": date,
                    "processed_indents": 0,
                    "created_adjusted_indents": 0,
                    "indents_with_shortfall": 0,
                    "indents_without_shortfall": 0,
                    "already_adjusted_indents": 0,
                    "details": []
                }
            }
        
        # Track processing results
        results = {
            "test_date": date,
            "processed_indents": 0,
            "created_adjusted_indents": 0,
            "indents_with_shortfall": 0,
            "indents_without_shortfall": 0,
            "already_adjusted_indents": 0,
            "details": []
        }
        
        # Process each indent
        for indent_data in indents:
            print(f"\n--- TEST Processing indent: {indent_data['name']} ---")
            try:
                indent_result = process_indent_for_shortfall(indent_data, date)
                results["processed_indents"] += 1
                results["details"].append(indent_result)
                
                print(f"TEST Indent {indent_data['name']} result: {indent_result['status']}")
                
                if indent_result["status"] == "adjusted_indent_created":
                    results["created_adjusted_indents"] += 1
                    results["indents_with_shortfall"] += 1
                elif indent_result["status"] == "shortfall_but_already_adjusted":
                    results["already_adjusted_indents"] += 1
                    results["indents_with_shortfall"] += 1
                elif indent_result["status"] == "no_shortfall":
                    results["indents_without_shortfall"] += 1
                
            except Exception as e:
                print(f"TEST ERROR processing indent {indent_data['name']}: {str(e)}")
                print(f"TEST Error type: {type(e)}")
                print(f"TEST Error traceback: {frappe.get_traceback()}")
                frappe.logger().error(f"Error processing indent {indent_data['name']}: {str(e)}")
                results["details"].append({
                    "indent_name": indent_data["name"],
                    "status": "error",
                    "message": f"Error processing indent: {str(e)}"
                })
                results["processed_indents"] += 1
        
        print(f"\n=== TEST Final Results ===")
        print(f"Test date: {date}")
        print(f"Processed: {results['processed_indents']}")
        print(f"Created adjusted indents: {results['created_adjusted_indents']}")
        print(f"With shortfall: {results['indents_with_shortfall']}")
        print(f"Without shortfall: {results['indents_without_shortfall']}")
        print(f"Already adjusted: {results['already_adjusted_indents']}")
        
        return {
            "success": True,
            "status": "success",
            "message": f"Test completed for {date}. Processed {results['processed_indents']} indents. Created {results['created_adjusted_indents']} adjusted indents.",
            "data": results
        }
        
    except Exception as e:
        print(f"TEST CRITICAL ERROR in test_create_adjusted_indents for {date}: {str(e)}")
        print(f"TEST Error type: {type(e)}")
        print(f"TEST Error traceback: {frappe.get_traceback()}")
        frappe.logger().error(f"Error in test_create_adjusted_indents for {date}: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "message": f"Failed to test adjusted indents for {date}: {str(e)}"
        }


@frappe.whitelist()
def debug_indent_shortfall(indent_name: str) -> Dict[str, Any]:
    """
    Debug endpoint to check shortfall calculation for a specific indent
    
    Args:
        indent_name: Name of the indent to debug
    
    Returns:
        Dict containing debug information about the shortfall calculation
    """
    print(f"=== DEBUG: Analyzing shortfall for indent: {indent_name} ===")
    
    try:
        # Get indent data
        print("Getting indent document...")
        indent = frappe.get_doc("SF Indent Master", indent_name)
        print(f"Indent found: {indent.name}")
        print(f"Indent docstatus: {indent.docstatus}")
        print(f"Indent workflow state: {indent.workflow_state}")
        
        if indent.docstatus != 1:
            print("ERROR: Indent must be submitted")
            return {
                "success": False,
                "status": "error",
                "message": "Indent must be submitted"
            }
        
        # Get route and delivery points
        print("Getting route document...")
        route_doc = frappe.get_doc("SF Delivery Route Master", indent.delivery_route)
        print(f"Route: {route_doc.name}")
        print(f"Number of delivery points: {len(route_doc.delivery_points)}")
        
        # Get original indent items
        print("Getting original indent items...")
        original_indent_items = get_indent_items(indent_name)
        print(f"Original indent items: {original_indent_items}")
        
        # Get aggregated sales order quantities for all delivery points
        print("Getting aggregated sales orders...")
        aggregated_sales_orders = get_aggregated_sales_orders_for_route(route_doc, indent.date)
        print(f"Aggregated sales orders: {aggregated_sales_orders}")
        
        # Calculate shortfall
        print("Calculating shortfall...")
        shortfall_items = calculate_shortfall(original_indent_items, aggregated_sales_orders)
        print(f"Shortfall items: {shortfall_items}")
        
        # Check if adjusted indent already exists
        print("Checking for existing adjusted indent...")
        existing_adjusted = frappe.get_value(
            "SF Indent Master",
            {
                "adjusted_indent_for": indent_name,
                "is_adjusted_indent": 1,
                "docstatus": ["!=", 2]  # Not cancelled
            },
            "name"
        )
        print(f"Existing adjusted indent: {existing_adjusted}")
        
        debug_data = {
            "indent_name": indent_name,
            "delivery_route": indent.delivery_route,
            "date": str(indent.date),
            "plant_warehouse": indent.get("for"),
            "delivery_points": [
                {
                    "drop_type": point.drop_type,
                    "drop_point": point.drop_point
                } for point in route_doc.delivery_points
            ],
            "original_indent_items": original_indent_items,
            "aggregated_sales_orders": aggregated_sales_orders,
            "shortfall_items": shortfall_items,
            "has_shortfall": bool(shortfall_items),
            "existing_adjusted_indent": existing_adjusted
        }
        
        print(f"=== DEBUG Summary ===")
        print(f"Has shortfall: {debug_data['has_shortfall']}")
        print(f"Number of shortfall items: {len(shortfall_items)}")
        print(f"Existing adjusted indent: {existing_adjusted}")
        
        return {
            "success": True,
            "status": "success",
            "message": "Debug information retrieved successfully",
            "data": debug_data
        }
        
    except Exception as e:
        print(f"DEBUG ERROR for {indent_name}: {str(e)}")
        print(f"DEBUG Error type: {type(e)}")
        print(f"DEBUG Error traceback: {frappe.get_traceback()}")
        frappe.logger().error(f"Error in debug_indent_shortfall for {indent_name}: {str(e)}")
        return {
            "success": False,
            "status": "error",
            "message": f"Failed to debug indent shortfall: {str(e)}"
        }

