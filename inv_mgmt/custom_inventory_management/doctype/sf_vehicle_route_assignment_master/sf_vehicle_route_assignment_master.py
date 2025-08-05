# Copyright (c) 2025, Hopnet Communications LLP and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class SFVehicleRouteAssignmentMaster(Document):
	def autoname(self):
		"""Generate unique name based on assignment type, date and vehicle"""
		if not self.assignment_type or not self.vehicle:
			return
		
		# Use vehicle ID directly for naming
		vehicle_id = self.vehicle
		
		# Clean vehicle ID for URL safety
		vehicle_id = frappe.scrub(vehicle_id)
		#make vehicle ID all caps
		vehicle_id = vehicle_id.upper()
		
		if self.assignment_type == "Daily" and self.assignment_date:
			# Format: SF-VRAM-Daily-2025-01-15-VehicleID
			date_str = self.assignment_date.strftime("%Y-%m-%d")
			self.name = f"SF-VRAM-{self.assignment_type}-{date_str}-{vehicle_id}"
		else:
			# Format: SF-VRAM-Fixed-VehicleID
			self.name = f"SF-VRAM-{self.assignment_type}-{vehicle_id}"
		
	def validate(self):
		"""Validate assignment data and check for conflicts"""
		self.validate_required_fields()
		self.check_duplicate_assignments()
	
	def validate_required_fields(self):
		"""Ensure required fields are filled based on assignment type"""
		if self.assignment_type == "Daily" and not self.assignment_date:
			frappe.throw("Assignment Date is required for Daily assignments")
	
	def check_duplicate_assignments(self):
		"""Check for duplicate assignments for same vehicle on same date"""
		filters = {
			"vehicle": self.vehicle,
			"name": ["!=", self.name]  # Exclude current record
		}
		
		if self.assignment_type == "Daily":
			filters["assignment_date"] = self.assignment_date
			filters["assignment_type"] = "Daily"
		else:
			filters["assignment_type"] = "Fixed"
		
		existing = frappe.db.exists("SF Vehicle Route Assignment Master", filters)
		if existing:
			assignment_type = "on the same date" if self.assignment_type == "Daily" else "as a fixed assignment"
			frappe.throw(f"Vehicle {self.vehicle} is already assigned {assignment_type}")