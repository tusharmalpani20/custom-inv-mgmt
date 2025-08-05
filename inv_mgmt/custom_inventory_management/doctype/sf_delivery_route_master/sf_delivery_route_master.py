# Copyright (c) 2025, Hopnet Communications LLP and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class SFDeliveryRouteMaster(Document):
	def validate(self):
		self.validate_delivery_points()
	
	def validate_delivery_points(self):
		"""Validate that all delivery points have drop_type as either Customer or Warehouse"""
		allowed_types = ["Customer", "Warehouse"]
		allowed_warehouse_categories = ["Plant", "Distribution Center", "Darkstore"]
		
		if self.delivery_points:
			for delivery_point in self.delivery_points:
				if delivery_point.drop_type and delivery_point.drop_type not in allowed_types:
					frappe.throw(
						f"Invalid Drop Type '{delivery_point.drop_type}' in delivery point. "
						f"Only 'Customer' or 'Warehouse' are allowed."
					)
				
				# Validate that drop_point is provided when drop_type is selected
				if delivery_point.drop_type and not delivery_point.drop_point:
					frappe.throw(
						f"Drop Point is required when Drop Type is '{delivery_point.drop_type}'."
					)
				
				# Validate that the drop_point record exists in the system
				if delivery_point.drop_point:
					if not frappe.db.exists(delivery_point.drop_type, delivery_point.drop_point):
						frappe.throw(
							f"'{delivery_point.drop_point}' does not exist in {delivery_point.drop_type} doctype."
						)
				
				# Validate warehouse category if drop_type is Warehouse
				if delivery_point.drop_type == "Warehouse" and delivery_point.drop_point:
					warehouse_category = frappe.db.get_value("Warehouse", delivery_point.drop_point, "custom_warehouse_category")
					if not warehouse_category:
						frappe.throw(
							f"Warehouse '{delivery_point.drop_point}' does not have a custom_warehouse_category set."
						)
					if warehouse_category not in allowed_warehouse_categories:
						frappe.throw(
							f"Invalid warehouse '{delivery_point.drop_point}' selected. "
							f"Only warehouses with category 'Plant', 'Distribution Center', or 'Darkstore' are allowed. "
							f"Current category: {warehouse_category}"
						)
