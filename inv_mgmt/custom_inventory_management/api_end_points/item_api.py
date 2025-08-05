import frappe
from frappe import _

@frappe.whitelist()
def get_sku_items():

    return frappe.get_all("Item", 

        filters={"has_variants": 0, "disabled": 0 , "is_stock_item" : 1},

        fields=["item_code", "item_name"],

        limit_page_length=10000,

        ignore_permissions=True

    )

@frappe.whitelist()
def get_holidays_for_week(start_date, end_date):
    """
    Get holidays for a specific date range
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        List of holidays with date and description
    """
    try:
        holidays = frappe.db.sql("""
            SELECT holiday_date, description 
            FROM `tabHoliday` 
            WHERE parent = "For Demand And Planning" 
            AND holiday_date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY holiday_date ASC
        """, {
            "start_date": start_date,
            "end_date": end_date
        }, as_dict=True)
        
        return holidays
        
    except Exception as e:
        frappe.log_error(f"Error getting holidays: {str(e)}", "Holiday API Error")
        return []