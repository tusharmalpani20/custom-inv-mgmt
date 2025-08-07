from erpnext.setup.doctype.branch.branch import Branch as ERPNextBranch
import frappe
from frappe import _
from datetime import datetime
import csv
import io
from frappe.utils.file_manager import save_file

class CustomBranch(ERPNextBranch):
    def on_submit(self):
        """Create Missing SKU and Damaged Items warehouses when branch is submitted"""
        super().on_submit()
        
        # Create Missing SKU warehouse
        self.create_missing_sku_warehouse()
        
        # Create Damaged SKU warehouse
        self.create_damaged_sku_warehouse()
    
    def create_missing_sku_warehouse(self):
        """Create Missing SKU warehouse for the branch"""
        warehouse_name = f"Missing SKU-{self.branch}"
        
        # Check if warehouse already exists
        if frappe.db.exists("Warehouse", warehouse_name):
            return
        
        warehouse_doc = frappe.get_doc({
            "doctype": "Warehouse",
            "warehouse_name": f"Missing SKU-{self.branch}",
            "name": warehouse_name,
            "company": "SIDS FARM PRIVATE LIMITED",
            "custom_branch": self.branch,
            "is_group": 0,
            "is_rejected_warehouse": 1,
            "parent_warehouse": "Missed SKU - SFPL",
            "warehouse_type": None,
            "disabled": 0,
            "docstatus": 0
        })
        
        warehouse_doc.insert(ignore_permissions=True)
        frappe.msgprint(f"Created Missing SKU warehouse: {warehouse_name}")
    
    def create_damaged_sku_warehouse(self):
        """Create Damaged SKU warehouse for the branch"""
        warehouse_name = f"Damaged SKU-{self.branch}"
        
        # Check if warehouse already exists
        if frappe.db.exists("Warehouse", warehouse_name):
            return
        
        warehouse_doc = frappe.get_doc({
            "doctype": "Warehouse",
            "warehouse_name": f"Damaged SKU-{self.branch}",
            "name": warehouse_name,
            "company": "SIDS FARM PRIVATE LIMITED",
            "custom_branch": self.branch,
            "is_group": 0,
            "is_rejected_warehouse": 1,
            "parent_warehouse": "Damaged SKU - SFPL",
            "warehouse_type": None,
            "disabled": 0,
            "docstatus": 0
        })
        
        warehouse_doc.insert(ignore_permissions=True)
        frappe.msgprint(f"Created Damaged SKU warehouse: {warehouse_name}")
