# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import json
import frappe
from frappe.utils import now_datetime

def enqueue(method, queue="default", timeout=None, is_async=True, **kwargs):
	"""
	Replacement for frappe.enqueue. 
	Instead of enqueuing directly to Redis, it creates a Controller Job record in MariaDB.
	"""
	# Find or create the Controller Job Type for this method
	job_type_name = frappe.db.exists("Controller Job Type", {"method": method})
	if not job_type_name:
		# If not registered in hooks, we create a default one
		job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": method,
			"create_log": 1
		}).insert(ignore_permissions=True)
		job_type_name = job_type.name

	# Create the Controller Job (the task instance)
	job = frappe.get_doc({
		"doctype": "Controller Job",
		"job_type": job_type_name,
		"job_name": method,
		"queue": queue,
		"status": "Queued",
		"arguments": json.dumps(kwargs, default=str),
		"timeout": timeout
	})
	job.insert(ignore_permissions=True)
	
	# If site is initialized, commit so the daemon can see it immediately
	if frappe.db:
		frappe.db.commit()
		
	return job.name
