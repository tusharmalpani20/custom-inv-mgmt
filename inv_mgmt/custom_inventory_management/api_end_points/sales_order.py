import frappe
from frappe import _
from frappe.model.document import Document
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List, Optional
from custom_app_api.custom_api.api_end_points.attendance_api import verify_dp_token, handle_error_response

# Debug flag - set to True to enable debug print statements
DEBUG = True

def debug_print(message: str):
    """Print debug messages only when DEBUG flag is True"""
    if DEBUG:
        print(f"[DEBUG] {message}")

def error_print(message: str):
    """Print error messages - always visible"""
    print(f"[ERROR] {message}")

def info_print(message: str):
    """Print info messages - always visible"""
    print(f"[INFO] {message}")


def get_internal_customer():
    """
    Get internal customer for default company
    """
    default_company = frappe.defaults.get_defaults().get("company")
    internal_customer = frappe.get_value("Customer", {"is_internal_customer": 1, "represents_company": default_company}, "name")
    
    if not internal_customer:
        frappe.throw(_("No internal customer found for company {0}").format(default_company))
    
    return internal_customer


def determine_effective_date(override_date: str = None) -> str:
    """
    Determine the effective date for order processing based on current time or override
    
    Args:
        override_date: Optional date override in YYYY-MM-DD format
    
    Returns:
        Effective date in YYYY-MM-DD format
    """
    if override_date:
        debug_print(f"Using override date: {override_date}")
        return override_date
    
    current_time = datetime.now()
    current_hour = current_time.hour
    
    # If current hour is between 0-5 (midnight to 5 AM), use previous day
    # Otherwise use today
    if 0 <= current_hour <= 5:
        effective_date = (current_time - timedelta(days=1)).date()
        debug_print(f"Current hour {current_hour} is between 0-5, using previous day: {effective_date}")
    else:
        effective_date = current_time.date()
        debug_print(f"Current hour {current_hour} is after 5, using today: {effective_date}")
    
    return str(effective_date)


@frappe.whitelist(allow_guest=True)
def get_driver_delivery_routes_with_sales_orders(override_date: str = None) -> Dict[str, Any]:
    """
    Get delivery routes with customers, warehouses, and their sales orders for the logged-in driver
    
    Args:
        override_date: Optional date parameter for testing (YYYY-MM-DD format)
    
    Required header: Auth-Token
    Returns:
        Dict containing delivery route info with customers, warehouses and sales orders
    """
    try:
        info_print("Starting get_driver_delivery_routes_with_sales_orders")
        
        # Determine effective date for processing
        effective_date = determine_effective_date(override_date)
        info_print(f"Using effective date: {effective_date}")
        
        # Verify token and authenticate
        is_valid, result = verify_dp_token(frappe.request.headers)
        if not is_valid:
            error_print("Token verification failed")
            frappe.local.response['http_status_code'] = 401
            return result
        
        employee_id = result["employee"]
        info_print(f"Authenticated employee: {employee_id}")
        
        # Step 1: Check if employee has a Driver record
        driver_record = get_driver_record_for_employee(employee_id)
        if not driver_record:
            error_print(f"No active driver record found for employee: {employee_id}")
            frappe.local.response['http_status_code'] = 403
            return {
                "success": False,
                "status": "error",
                "message": "Access denied. No active driver record found for this employee.",
                "code": "NO_DRIVER_RECORD",
                "http_status_code": 403
            }
        
        debug_print(f"Found driver record: {driver_record}")
        
        # Step 2: Get vehicle assigned to the driver
        vehicle_record = get_vehicle_for_driver(driver_record)
        if not vehicle_record:
            error_print(f"No vehicle assigned to driver: {driver_record}")
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": "No orders found. No vehicle assigned to this driver.",
                "code": "NO_ORDERS_FOUND",
                "http_status_code": 404
            }
        
        debug_print(f"Found vehicle: {vehicle_record}")
        
        # Step 3: Get vehicle route assignment
        route_assignment = get_vehicle_route_assignment(vehicle_record, effective_date)
        if not route_assignment:
            error_print(f"No route assignment found for vehicle: {vehicle_record}")
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error", 
                "message": "No orders found. No route assignment found for this vehicle.",
                "code": "NO_ORDERS_FOUND",
                "http_status_code": 404
            }
        
        debug_print(f"Found route assignment: {route_assignment}")
        
        # Step 4: Get all delivery routes for this assignment
        delivery_routes = get_delivery_routes_from_assignment(route_assignment)
        if not delivery_routes:
            error_print(f"No delivery routes found in assignment: {route_assignment}")
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": "No orders found. No delivery routes found in route assignment.",
                "code": "NO_ORDERS_FOUND", 
                "http_status_code": 404
            }
        
        info_print(f"Found {len(delivery_routes)} delivery routes")
        
        # Step 5: Process each delivery route to get customers, warehouses and sales orders
        processed_routes = []
        for route in delivery_routes:
            route_data = process_delivery_route_with_sales_orders(route, effective_date, employee_id)
            if route_data:
                processed_routes.append(route_data)
        
        info_print(f"Processed {len(processed_routes)} routes with sales orders")
        
        # Get company and internal customer info
        default_company = frappe.defaults.get_defaults().get("company")
        internal_customer = get_internal_customer()
        
        frappe.local.response['http_status_code'] = 200
        return {
            "success": True,
            "status": "success",
            "message": "Data fetched successfully",
            "code": "SUCCESS",
            "data": {
                "effective_date": effective_date,
                "driver": {
                    "employee_id": employee_id,
                    "driver_record": driver_record,
                    "vehicle": vehicle_record
                },
                "route_assignment": route_assignment,
                "company": default_company,
                "internal_customer": internal_customer,
                "delivery_routes": processed_routes,
                "total_routes": len(processed_routes)
            },
            "http_status_code": 200
        }
        
    except Exception as e:
        error_print(f"Error in get_driver_delivery_routes_with_sales_orders: {str(e)}")
        frappe.log_error(str(e), "Driver Delivery Routes with Sales Orders API Error")
        frappe.local.response['http_status_code'] = 500
        return {
            "success": False,
            "status": "error",
            "message": "Failed to fetch delivery route data with sales orders",
            "code": "SYSTEM_ERROR",
            "error": str(e),
            "http_status_code": 500
        }


def get_driver_record_for_employee(employee_id: str) -> Optional[str]:
    """
    Get active driver record for the given employee
    
    Args:
        employee_id: Employee ID
    
    Returns:
        Driver record name if found, None otherwise
    """
    debug_print(f"Looking for driver record for employee: {employee_id}")
    
    try:
        driver_record = frappe.db.get_value(
            "Driver",
            {
                "employee": employee_id,
                "status": "Active"
            },
            "name"
        )
        
        if driver_record:
            debug_print(f"Found active driver record: {driver_record}")
            return driver_record
        else:
            debug_print(f"No active driver record found for employee: {employee_id}")
            return None
            
    except Exception as e:
        error_print(f"Error getting driver record for employee {employee_id}: {str(e)}")
        return None


def get_vehicle_for_driver(driver_record: str) -> Optional[str]:
    """
    Get vehicle assigned to the driver
    
    Note: If multiple vehicles are assigned to the same driver, we always consider the first one
    
    Args:
        driver_record: Driver record name
    
    Returns:
        Vehicle name if found, None otherwise
    """
    debug_print(f"Looking for vehicle assigned to driver: {driver_record}")
    
    try:
        vehicles = frappe.db.sql("""
            SELECT name
            FROM `tabVehicle`
            WHERE custom_driver = %(driver)s
            ORDER BY creation ASC
            LIMIT 1
        """, {"driver": driver_record}, as_dict=True)
        
        if vehicles:
            vehicle = vehicles[0].name
            debug_print(f"Found vehicle assigned to driver: {vehicle}")
            if len(vehicles) > 1:
                debug_print(f"Note: Multiple vehicles found for driver {driver_record}, using first one: {vehicle}")
            return vehicle
        else:
            debug_print(f"No vehicle assigned to driver: {driver_record}")
            return None
            
    except Exception as e:
        error_print(f"Error getting vehicle for driver {driver_record}: {str(e)}")
        return None


def get_vehicle_route_assignment(vehicle: str, effective_date: str) -> Optional[str]:
    """
    Get vehicle route assignment for the given vehicle and date
    
    First checks for Daily assignment for the effective date
    If not found, checks for Fixed assignment
    
    Args:
        vehicle: Vehicle name
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        SF Vehicle Route Assignment Master name if found, None otherwise
    """
    debug_print(f"Looking for route assignment for vehicle: {vehicle}, date: {effective_date}")
    
    try:
        # First check for Daily assignment
        daily_assignment = frappe.db.get_value(
            "SF Vehicle Route Assignment Master",
            {
                "vehicle": vehicle,
                "assignment_type": "Daily",
                "assignment_date": effective_date,
                "status": "Active"
            },
            "name"
        )
        
        if daily_assignment:
            debug_print(f"Found Daily route assignment: {daily_assignment}")
            return daily_assignment
        
        # If no Daily assignment found, check for Fixed assignment
        fixed_assignment = frappe.db.get_value(
            "SF Vehicle Route Assignment Master",
            {
                "vehicle": vehicle,
                "assignment_type": "Fixed",
                "status": "Active"
            },
            "name"
        )
        
        if fixed_assignment:
            debug_print(f"Found Fixed route assignment: {fixed_assignment}")
            return fixed_assignment
        
        debug_print(f"No route assignment found for vehicle: {vehicle}")
        return None
        
    except Exception as e:
        error_print(f"Error getting route assignment for vehicle {vehicle}: {str(e)}")
        return None


def get_delivery_routes_from_assignment(assignment_name: str) -> List[str]:
    """
    Get all delivery routes from the route assignment
    
    Args:
        assignment_name: SF Vehicle Route Assignment Master name
    
    Returns:
        List of SF Delivery Route Master names in order
    """
    debug_print(f"Getting delivery routes from assignment: {assignment_name}")
    
    try:
        route_details = frappe.db.sql("""
            SELECT route
            FROM `tabSF Vehicle Route Assignment Detail`
            WHERE parent = %(assignment)s
            ORDER BY idx ASC
        """, {"assignment": assignment_name}, as_dict=True)
        
        routes = [detail.route for detail in route_details]
        debug_print(f"Found {len(routes)} routes in assignment: {routes}")
        return routes
        
    except Exception as e:
        error_print(f"Error getting delivery routes from assignment {assignment_name}: {str(e)}")
        return []


def get_indent_details_for_route(delivery_route: str, effective_date: str, employee_id: str) -> Optional[Dict]:
    """
    Get indent details for a specific delivery route, date and employee
    
    Args:
        delivery_route: SF Delivery Route Master name
        effective_date: Date in YYYY-MM-DD format
        employee_id: Employee ID
    
    Returns:
        Dict containing indent details if found, None otherwise
    """
    debug_print(f"Getting indent details for route: {delivery_route}, date: {effective_date}, employee: {employee_id}")
    
    try:
        # Get indent records matching the criteria
        indent_records = frappe.db.sql("""
            SELECT name, delivery_route, vehicle, driver, `for`, date, company,
                   trip_started_at, trip_started_by, docstatus, workflow_state
            FROM `tabSF Indent Master`
            WHERE delivery_route = %(route)s
            AND date = %(date)s
            AND driver = %(employee)s
            AND docstatus = 1
            ORDER BY creation DESC
            LIMIT 1
        """, {
            "route": delivery_route,
            "date": effective_date,
            "employee": employee_id
        }, as_dict=True)
        
        if not indent_records:
            debug_print(f"No submitted indent found for route: {delivery_route}, date: {effective_date}, employee: {employee_id}")
            return None
        
        indent_record = indent_records[0]
        debug_print(f"Found indent: {indent_record.name}")
        
        # # Get indent items
        # indent_items = frappe.db.sql("""
        #     SELECT sku, uom, quantity, crates, loose, difference, actual
        #     FROM `tabSF Indent Item`
        #     WHERE parent = %(parent)s
        #     ORDER BY idx ASC
        # """, {"parent": indent_record.name}, as_dict=True)
        
        # # Get item details for each indent item
        # for item in indent_items:
        #     if item.sku:
        #         item_details = frappe.db.get_value("Item", item.sku, ["item_name", "item_group", "stock_uom"], as_dict=True)
        #         if item_details:
        #             item["item_name"] = item_details.item_name
        #             item["item_group"] = item_details.item_group
        #             item["stock_uom"] = item_details.stock_uom
        
        indent_data = {
            "indent_name": indent_record.name,
            "delivery_route": indent_record.delivery_route,
            "vehicle": indent_record.vehicle,
            "driver": indent_record.driver,
            "for_warehouse": indent_record.get("for"),
            "date": str(indent_record.date),
            "company": indent_record.company,
            "docstatus": indent_record.docstatus,
            "workflow_state": indent_record.workflow_state,
            "trip_started_at": str(indent_record.trip_started_at) if indent_record.trip_started_at else None,
            "trip_started_by": indent_record.trip_started_by,
            # "items": indent_items,
            # "total_items": len(indent_items),
            # "total_quantity": sum(item.quantity for item in indent_items if item.quantity),
            # "total_crates": sum(item.crates for item in indent_items if item.crates),
            # "total_loose": sum(item.loose for item in indent_items if item.loose),
            # "total_actual": sum(item.actual for item in indent_items if item.actual)
        }
        
        # debug_print(f"Processed indent {indent_record.name} with {len(indent_items)} items")
        return indent_data
        
    except Exception as e:
        error_print(f"Error getting indent details for route {delivery_route}: {str(e)}")
        return None


def process_delivery_route_with_sales_orders(route_name: str, effective_date: str, employee_id: str = None) -> Optional[Dict]:
    """
    Process a delivery route to get customers, warehouses and their sales orders
    
    Args:
        route_name: SF Delivery Route Master name
        effective_date: Date in YYYY-MM-DD format
        employee_id: Employee ID for indent lookup
    
    Returns:
        Dict containing route data with customers, warehouses and sales orders
    """
    debug_print(f"Processing delivery route: {route_name}")
    
    try:
        # Get delivery route document
        route_doc = frappe.get_doc("SF Delivery Route Master", route_name)
        
        # Get start point warehouse details
        start_point_warehouse = get_warehouse_details_with_sales_orders(route_doc.start_point, effective_date)
        
        # Process delivery points (customers and warehouses)
        delivery_points = []
        for point in route_doc.delivery_points:
            debug_print(f"Processing delivery point - Drop Type: {point.drop_type}, Drop Point: {point.drop_point}")
            
            if point.drop_type == "Customer":
                # Handle customer delivery point
                customer_data = get_customer_details_with_sales_orders_from_delivery_point(point, effective_date)
                if customer_data:
                    delivery_points.append(customer_data)
            elif point.drop_type == "Warehouse":
                # Handle warehouse delivery point
                warehouse_data = get_warehouse_details_with_sales_orders_from_delivery_point(point, effective_date)
                if warehouse_data:
                    delivery_points.append(warehouse_data)
            else:
                debug_print(f"Skipping delivery point with unsupported drop_type: {point.drop_type}")
        
        # Get indent details for this route
        indent_details = None
        if employee_id:
            indent_details = get_indent_details_for_route(route_name, effective_date, employee_id)
        
        route_data = {
            "route_name": route_doc.name,
            "route_display_name": route_doc.route_name,
            "route_category": route_doc.route_category,
            "branch": route_doc.branch,
            "start_point_warehouse": start_point_warehouse,
            "delivery_points": delivery_points,
            "total_delivery_points": len(delivery_points),
            "indent": indent_details
        }
        
        debug_print(f"Processed route {route_name} with {len(delivery_points)} delivery points and indent: {indent_details is not None}")
        return route_data
        
    except Exception as e:
        error_print(f"Error processing delivery route {route_name}: {str(e)}")
        return None


def get_warehouse_details_with_sales_orders(warehouse_name: str, effective_date: str) -> Dict:
    """
    Get warehouse details along with sales orders for internal customer
    Note: Skips sales orders for Plant warehouses since they don't have customers
    
    Args:
        warehouse_name: Warehouse name
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        Dict containing warehouse details and sales orders (if applicable)
    """
    debug_print(f"Getting warehouse details with sales orders for: {warehouse_name}")
    
    try:
        # Get warehouse document
        warehouse_doc = frappe.get_doc("Warehouse", warehouse_name)
        
        # Check warehouse category to determine if we should fetch sales orders
        warehouse_category = getattr(warehouse_doc, 'custom_warehouse_category', None)
        debug_print(f"Warehouse {warehouse_name} has category: {warehouse_category}")
        
        # Get warehouse address
        warehouse_address = None
        if hasattr(warehouse_doc, 'address_line_1') and warehouse_doc.address_line_1:
            warehouse_address = {
                "address_line_1": warehouse_doc.address_line_1,
                "address_line_2": warehouse_doc.address_line_2,
                "city": warehouse_doc.city,
                "state": warehouse_doc.state,
                "pincode": warehouse_doc.pin,
                "latitude": getattr(warehouse_doc, 'custom_latitude', None),
                "longitude": getattr(warehouse_doc, 'custom_longitude', None)
            }
        
        # Only get sales orders if warehouse is not a Plant
        sales_orders = []
        if warehouse_category == 'Plant':
            debug_print(f"Skipping sales order fetch for Plant warehouse: {warehouse_name}")
            sales_orders = None  # Set to None to indicate this warehouse doesn't have customers
        else:
            # Get internal customer and fetch sales orders
            internal_customer = get_internal_customer()
            sales_orders = get_sales_orders_for_warehouse(warehouse_name, internal_customer, effective_date)
        
        warehouse_data = {
            "name": warehouse_doc.name,
            "display_name": warehouse_doc.warehouse_name,
            "type": getattr(warehouse_doc, 'warehouse_type', None),
            "category": warehouse_category,
            "branch": getattr(warehouse_doc, 'custom_branch', None),
            "address": warehouse_address,
            "sales_orders": sales_orders,
            "total_sales_orders": len(sales_orders) if sales_orders else 0,
            "is_plant": warehouse_category == 'Plant'
        }
        
        debug_print(f"Got warehouse details for {warehouse_name} with {len(sales_orders or [])} sales orders (Plant: {warehouse_category == 'Plant'})")
        return warehouse_data
        
    except Exception as e:
        error_print(f"Error getting warehouse details for {warehouse_name}: {str(e)}")
        return {
            "warehouse_name": warehouse_name,
            "error": str(e),
            "sales_orders": None,
            "total_sales_orders": 0,
            "is_plant": False
        }


def get_customer_details_with_sales_orders_from_delivery_point(delivery_point: object, effective_date: str) -> Optional[Dict]:
    """
    Get customer details along with sales orders from delivery point
    
    Args:
        delivery_point: SF Delivery Point object from route
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        Dict containing customer details and sales orders
    """
    # Validate customer field first
    if not hasattr(delivery_point, 'drop_point') or not delivery_point.drop_point:
        debug_print(f"Skipping delivery point - no drop_point specified: {getattr(delivery_point, 'name', 'Unknown')}")
        return None
    
    debug_print(f"Getting customer details with sales orders for: {delivery_point.drop_point}")
    
    try:
        # Check if customer exists before trying to get document
        if not frappe.db.exists("Customer", delivery_point.drop_point):
            error_print(f"Customer {delivery_point.drop_point} does not exist in system")
            return None
        
        # Get customer document
        customer_doc = frappe.get_doc("Customer", delivery_point.drop_point)
        
        # Get shipping address details if available
        shipping_address = None
        if hasattr(customer_doc, 'custom_customer_shipping_address') and customer_doc.custom_customer_shipping_address:
            try:
                address_doc = frappe.get_doc("Address", customer_doc.custom_customer_shipping_address)
                shipping_address = {
                    "name": address_doc.name,
                    "address_type": address_doc.address_type,
                    "address_line1": address_doc.address_line1,
                    "address_line2": address_doc.address_line2,
                    "city": address_doc.city,
                    "state": address_doc.state,
                    "pincode": address_doc.pincode,
                    "country": address_doc.country,
                    "latitude": getattr(address_doc, 'custom_latitude', None),
                    "longitude": getattr(address_doc, 'custom_longitude', None)
                }
                debug_print(f"Found shipping address for customer {delivery_point.drop_point}: {address_doc.name}")
            except Exception as addr_e:
                error_print(f"Error getting shipping address {customer_doc.custom_customer_shipping_address}: {str(addr_e)}")
                shipping_address = None
        
        # Get sales orders for this customer
        sales_orders = get_sales_orders_for_customer(delivery_point.drop_point, effective_date)
        
        customer_data = {
            "entity_type": "Customer",
            "name": customer_doc.name,
            "display_name": customer_doc.customer_name,
            "type": customer_doc.customer_type,
            "group": customer_doc.customer_group,
            "territory": customer_doc.territory,
            "address": shipping_address,
            "sales_orders": sales_orders,
            "total_sales_orders": len(sales_orders) if sales_orders else 0
        }
        
        debug_print(f"Got customer details for {delivery_point.drop_point} with {len(sales_orders or [])} sales orders")
        return customer_data
        
    except Exception as e:
        error_print(f"Error getting customer details for {delivery_point.drop_point}: {str(e)}")
        return None


def get_warehouse_details_with_sales_orders_from_delivery_point(delivery_point: object, effective_date: str) -> Optional[Dict]:
    """
    Get warehouse details along with sales orders from delivery point
    
    Args:
        delivery_point: SF Delivery Point object from route
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        Dict containing warehouse details and sales orders
    """
    # Validate warehouse field first
    if not hasattr(delivery_point, 'drop_point') or not delivery_point.drop_point:
        debug_print(f"Skipping delivery point - no drop_point specified: {getattr(delivery_point, 'name', 'Unknown')}")
        return None
    
    debug_print(f"Getting warehouse details with sales orders for delivery point: {delivery_point.drop_point}")
    
    try:
        # Check if warehouse exists before trying to get document
        if not frappe.db.exists("Warehouse", delivery_point.drop_point):
            error_print(f"Warehouse {delivery_point.drop_point} does not exist in system")
            return None
        
        # Get warehouse details using existing function
        warehouse_data = get_warehouse_details_with_sales_orders(delivery_point.drop_point, effective_date)
        
        # Add entity type to distinguish from customers
        warehouse_data["entity_type"] = "Warehouse"
        
        debug_print(f"Got warehouse details for delivery point {delivery_point.drop_point}")
        return warehouse_data
        
    except Exception as e:
        error_print(f"Error getting warehouse details for delivery point {delivery_point.drop_point}: {str(e)}")
        return None


def get_customer_details_with_sales_orders(delivery_point: object, effective_date: str) -> Optional[Dict]:
    """
    Get customer details along with sales orders (Legacy function for backward compatibility)
    
    Args:
        delivery_point: SF Delivery Point object from route
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        Dict containing customer details and sales orders
    """
    # Validate customer field first
    if not hasattr(delivery_point, 'customer') or not delivery_point.customer:
        debug_print(f"Skipping delivery point - no customer specified: {getattr(delivery_point, 'name', 'Unknown')}")
        return None
    
    debug_print(f"Getting customer details with sales orders for: {delivery_point.customer}")
    
    try:
        # Check if customer exists before trying to get document
        if not frappe.db.exists("Customer", delivery_point.customer):
            error_print(f"Customer {delivery_point.customer} does not exist in system")
            return None
        
        # Get customer document
        customer_doc = frappe.get_doc("Customer", delivery_point.customer)
        
        # Get address details if specified
        address_details = None
        if hasattr(delivery_point, 'address') and delivery_point.address:
            try:
                address_doc = frappe.get_doc("Address", delivery_point.address)
                address_details = {
                    "name": address_doc.name,
                    "address_type": address_doc.address_type,
                    "address_line1": address_doc.address_line1,
                    "address_line2": address_doc.address_line2,
                    "city": address_doc.city,
                    "state": address_doc.state,
                    "pincode": address_doc.pincode,
                    "country": address_doc.country,
                    "latitude": getattr(address_doc, 'custom_latitude', None),
                    "longitude": getattr(address_doc, 'custom_longitude', None)
                }
            except Exception as addr_e:
                error_print(f"Error getting address {delivery_point.address}: {str(addr_e)}")
                address_details = None
        
        # Get sales orders for this customer
        sales_orders = get_sales_orders_for_customer(delivery_point.customer, effective_date)
        
        customer_data = {
            "entity_type": "Customer",
            "customer": customer_doc.name,
            "customer_name": customer_doc.customer_name,
            "customer_type": customer_doc.customer_type,
            "customer_group": customer_doc.customer_group,
            "territory": customer_doc.territory,
            "customer_category": getattr(delivery_point, 'customer_category', None),
            "address": address_details,
            "sales_orders": sales_orders,
            "total_sales_orders": len(sales_orders) if sales_orders else 0
        }
        
        debug_print(f"Got customer details for {delivery_point.customer} with {len(sales_orders or [])} sales orders")
        return customer_data
        
    except Exception as e:
        error_print(f"Error getting customer details for {delivery_point.customer}: {str(e)}")
        return None


def get_sales_orders_for_warehouse(warehouse_name: str, internal_customer: str, effective_date: str) -> List[Dict]:
    """
    Get sales orders for warehouse using SF Facility Master's shipping address
    
    Args:
        warehouse_name: Warehouse name
        internal_customer: Internal customer name
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        List of sales order dictionaries
    """
    debug_print(f"Getting sales orders for warehouse {warehouse_name} with internal customer {internal_customer}")
    
    try:
        # Get shipping address from SF Facility Master
        facility_data = frappe.db.get_value(
            "SF Facility Master",
            {"warehouse": warehouse_name},
            ["shipping_address", "facility_name", "facility_id"],
            as_dict=True
        )
        
        if not facility_data:
            debug_print(f"No SF Facility Master record found for warehouse {warehouse_name}")
            return []
        
        if not facility_data.shipping_address:
            debug_print(f"No shipping address found in SF Facility Master for warehouse {warehouse_name}")
            return []
        
        shipping_address = facility_data.shipping_address
        debug_print(f"Found shipping address {shipping_address} for warehouse {warehouse_name} from SF Facility Master")
        
        # Get sales orders for internal customer with this specific shipping address
        sales_orders = frappe.db.sql("""
            SELECT name, customer, customer_name, transaction_date, delivery_date,
                   grand_total, status, docstatus, set_warehouse, shipping_address_name,
                   shipping_address, per_delivered, per_billed
            FROM `tabSales Order`
            WHERE customer = %(customer)s
            AND transaction_date = %(date)s
            AND shipping_address_name = %(shipping_address)s
            AND docstatus = 1
            ORDER BY creation DESC
        """, {
            "customer": internal_customer,
            "date": effective_date,
            "shipping_address": shipping_address
        }, as_dict=True)
        
        debug_print(f"Found {len(sales_orders)} sales orders for warehouse {warehouse_name}")
        
        # Get items for each sales order
        for order in sales_orders:
            order_items = frappe.db.sql("""
                SELECT item_code, item_name, qty, delivered_qty, billed_amt,
                       rate, amount, warehouse
                FROM `tabSales Order Item`
                WHERE parent = %(parent)s
                ORDER BY idx ASC
            """, {"parent": order.name}, as_dict=True)
            
            order["items"] = order_items
            order["total_items"] = len(order_items)
        
        return sales_orders
        
    except Exception as e:
        error_print(f"Error getting sales orders for warehouse {warehouse_name}: {str(e)}")
        return []


def get_sales_orders_for_customer(customer_name: str, effective_date: str) -> List[Dict]:
    """
    Get sales orders for customer
    
    Args:
        customer_name: Customer name
        effective_date: Date in YYYY-MM-DD format
    
    Returns:
        List of sales order dictionaries
    """
    debug_print(f"Getting sales orders for customer {customer_name}")
    
    try:
        # Get sales orders for this customer
        sales_orders = frappe.db.sql("""
            SELECT name, customer, customer_name, transaction_date, delivery_date,
                   grand_total, status, docstatus, set_warehouse, shipping_address_name,
                   shipping_address, per_delivered, per_billed
            FROM `tabSales Order`
            WHERE customer = %(customer)s
            AND transaction_date = %(date)s
            AND docstatus = 1
            ORDER BY creation DESC
        """, {
            "customer": customer_name,
            "date": effective_date
        }, as_dict=True)
        
        debug_print(f"Found {len(sales_orders)} sales orders for customer {customer_name}")
        
        # Get items for each sales order
        for order in sales_orders:
            order_items = frappe.db.sql("""
                SELECT item_code, item_name, qty, delivered_qty, billed_amt,
                       rate, amount, warehouse
                FROM `tabSales Order Item`
                WHERE parent = %(parent)s
                ORDER BY idx ASC
            """, {"parent": order.name}, as_dict=True)
            
            order["items"] = order_items
            order["total_items"] = len(order_items)
        
        return sales_orders
        
    except Exception as e:
        error_print(f"Error getting sales orders for customer {customer_name}: {str(e)}")
        return []


def get_crate_details_for_item(item_code: str, quantity: float) -> Dict[str, Any]:
    """
    Get crate details for a given item and quantity.
    
    This function calculates how many complete crates and loose items can be made
    from a given quantity, based on the UOM conversion factor.
    
    Args:
        item_code (str): The item code/SKU to check
        quantity (float): The total quantity to calculate crates for
    
    Returns:
        dict: A dictionary containing:
            - crates: Number of complete crates (0 if no crate conversion)
            - loose: Number of items that don't fit in complete crates (or total quantity if no crate conversion)
            - conversion_factor: Number of items per crate (0 if no crate conversion)
            - has_crate_conversion: Whether the item has crate conversion defined
            - message: Information message for user
    
    Example:
        If an item has 24 items per crate and quantity is 100:
        - crates will be 4 (100 // 24)
        - loose will be 4 (100 - (4 * 24))
        
        If an item has no crate conversion and quantity is 100:
        - crates will be 0
        - loose will be 100
    """
    try:
        quantity = float(quantity)
        
        # Get UOM conversion detail directly
        crate_conversion = frappe.get_all(
            "UOM Conversion Detail",
            filters={
                "parent": item_code,
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
                'message': None
            }
        
        # For items without crate conversion
        return {
            'crates': 0,
            'loose': quantity,  # All quantity goes to loose items
            'conversion_factor': 0,
            'has_crate_conversion': False,
            'message': f"No crate conversion found for item {item_code}. All quantity will be treated as loose items."
        }
        
    except Exception as e:
        error_print(f"Error calculating crates for item {item_code}: {str(e)}")
        return {
            'crates': 0,
            'loose': quantity,
            'conversion_factor': 0,
            'has_crate_conversion': False,
            'message': f"Error calculating crates: {str(e)}"
        }


@frappe.whitelist(allow_guest=True)
def get_sales_order_details_for_delivery(sales_order_id: str) -> Dict[str, Any]:
    """
    Get sales order details and delivery items for a specific sales order ID
    
    Args:
        sales_order_id: Sales Order ID/name
    
    Required header: Auth-Token
    Returns:
        Dict containing sales order details and delivery items
    """
    try:
        info_print(f"Starting get_sales_order_details_for_delivery for sales order: {sales_order_id}")
        
        # Verify token and authenticate
        is_valid, result = verify_dp_token(frappe.request.headers)
        if not is_valid:
            error_print("Token verification failed")
            frappe.local.response['http_status_code'] = 401
            return result
        
        employee_id = result["employee"]
        info_print(f"Authenticated employee: {employee_id}")
        
        # Check if sales order exists
        if not frappe.db.exists("Sales Order", sales_order_id):
            error_print(f"Sales Order {sales_order_id} does not exist")
            frappe.local.response['http_status_code'] = 404
            return {
                "success": False,
                "status": "error",
                "message": f"Sales Order {sales_order_id} not found",
                "code": "SALES_ORDER_NOT_FOUND",
                "http_status_code": 404
            }
        
        # Get sales order document
        sales_order_doc = frappe.get_doc("Sales Order", sales_order_id)
        
        # Check if sales order is submitted
        if sales_order_doc.docstatus != 1:
            error_print(f"Sales Order {sales_order_id} is not submitted (docstatus: {sales_order_doc.docstatus})")
            frappe.local.response['http_status_code'] = 400
            return {
                "success": False,
                "status": "error",
                "message": f"Sales Order {sales_order_id} is not submitted",
                "code": "SALES_ORDER_NOT_SUBMITTED",
                "http_status_code": 400
            }
        
        # Get customer details
        customer_doc = frappe.get_doc("Customer", sales_order_doc.customer)
        
        # Get shipping address details if available
        shipping_address = None
        if sales_order_doc.shipping_address_name:
            try:
                address_doc = frappe.get_doc("Address", sales_order_doc.shipping_address_name)
                shipping_address = {
                    "name": address_doc.name,
                    "address_type": address_doc.address_type,
                    "address_line1": address_doc.address_line1,
                    "address_line2": address_doc.address_line2,
                    "city": address_doc.city,
                    "state": address_doc.state,
                    "pincode": address_doc.pincode,
                    "country": address_doc.country,
                    "latitude": getattr(address_doc, 'custom_latitude', None),
                    "longitude": getattr(address_doc, 'custom_longitude', None)
                }
            except Exception as addr_e:
                error_print(f"Error getting shipping address {sales_order_doc.shipping_address_name}: {str(addr_e)}")
                shipping_address = None
        
        # Get delivery items (items that need to be delivered)
        delivery_items = []
        total_delivery_qty = 0
        total_delivered_qty = 0
        
        for item in sales_order_doc.items:
            # Calculate remaining delivery quantity
            remaining_qty = item.qty - item.delivered_qty
            
            if remaining_qty > 0:
                # Get crate details for this item
                crate_details = get_crate_details_for_item(item.item_code, remaining_qty)
                
                delivery_item = {
                    "item_code": item.item_code,
                    "item_name": item.item_name,
                    "qty": item.qty,
                    "delivered_qty": item.delivered_qty,
                    "remaining_qty": remaining_qty,
                    "rate": item.rate,
                    "amount": item.amount,
                    "warehouse": item.warehouse,
                    "uom": item.uom,
                    "description": item.description,
                    "needs_delivery": True,
                    # Add crate calculations
                    "crates": crate_details.get("crates", 0),
                    "loose": crate_details.get("loose", remaining_qty),
                    "conversion_factor": crate_details.get("conversion_factor", 0),
                    "has_crate_conversion": crate_details.get("has_crate_conversion", False),
                    "crate_message": crate_details.get("message", None)
                }
                delivery_items.append(delivery_item)
                total_delivery_qty += item.qty
                total_delivered_qty += item.delivered_qty
        
        # Get sales order summary
        sales_order_summary = {
            "name": sales_order_doc.name,
            "customer": sales_order_doc.customer,
            "customer_name": sales_order_doc.customer_name,
            "transaction_date": str(sales_order_doc.transaction_date),
            "delivery_date": str(sales_order_doc.delivery_date) if sales_order_doc.delivery_date else None,
            "grand_total": sales_order_doc.grand_total,
            "status": sales_order_doc.status,
            "docstatus": sales_order_doc.docstatus,
            "set_warehouse": sales_order_doc.set_warehouse,
            "shipping_address_name": sales_order_doc.shipping_address_name,
            "per_delivered": sales_order_doc.per_delivered,
            "per_billed": sales_order_doc.per_billed,
            "total_items": len(sales_order_doc.items),
            "delivery_items_count": len(delivery_items),
            "total_delivery_qty": total_delivery_qty,
            "total_delivered_qty": total_delivered_qty,
            "delivery_percentage": sales_order_doc.per_delivered
        }
        
        # Get customer details
        customer_details = {
            "customer": customer_doc.name,
            "customer_name": customer_doc.customer_name,
            "customer_type": customer_doc.customer_type,
            "customer_group": customer_doc.customer_group,
            "territory": customer_doc.territory,
            "shipping_address": shipping_address
        }
        
        frappe.local.response['http_status_code'] = 200
        return {
            "success": True,
            "status": "success",
            "message": "Sales order details fetched successfully",
            "code": "SUCCESS",
            "data": {
                "sales_order": sales_order_summary,
                "customer": customer_details,
                "delivery_items": delivery_items,
                "delivery_summary": {
                    "total_items_to_deliver": len(delivery_items),
                    "total_qty_to_deliver": total_delivery_qty,
                    "total_delivered_qty": total_delivered_qty,
                    "delivery_percentage": sales_order_doc.per_delivered
                }
            },
            "http_status_code": 200
        }
        
    except Exception as e:
        error_print(f"Error in get_sales_order_details_for_delivery: {str(e)}")
        frappe.log_error(str(e), "Sales Order Details for Delivery API Error")
        frappe.local.response['http_status_code'] = 500
        return {
            "success": False,
            "status": "error",
            "message": "Failed to fetch sales order details for delivery",
            "code": "SYSTEM_ERROR",
            "error": str(e),
            "http_status_code": 500
        }
