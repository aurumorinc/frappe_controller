# Copyright (c) 2021, Frappe Technologies and contributors
# License: MIT. See LICENSE

import json
import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime

class ControllerJobType(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		create_log: DF.Check
		last_execution: DF.Datetime | None
		method: DF.Data
		server_script: DF.Link | None
		stopped: DF.Check
		max_calls_per_minute: DF.Int
		concurrency_limit: DF.Int
	# end: auto-generated types

	def is_allowed_by_rate_limit(self) -> bool:
		if not self.max_calls_per_minute:
			return True
		
		# Simple atomic rate limiting using Redis
		cache = frappe.cache()
		key = f"controller_job_rate_limit:{self.name}"
		
		# incrby returns the value after increment
		new_count = cache.incrby(key, 1)
		
		if int(new_count) == 1:
			cache.expire(key, 60)
		
		if int(new_count) > self.max_calls_per_minute:
			return False
			
		return True

def sync_jobs(hooks: list | dict = None):
	frappe.reload_doc("controller", "doctype", "controller_job_type")
	frappe.reload_doc("controller", "doctype", "controller_job")
	
	raw_hooks = hooks if hooks is not None else frappe.get_hooks("controller_events")
	
	if not raw_hooks:
		return

	# Normalize raw_hooks to a list
	if isinstance(raw_hooks, dict):
		raw_hooks = [raw_hooks]

	defined_methods = []
	
	for hook_entry in raw_hooks:
		if isinstance(hook_entry, str):
			defined_methods.append(hook_entry)
			insert_single_event(hook_entry)
		elif isinstance(hook_entry, dict):
			# Case 1: {"method": "path", "max_calls_per_minute": 10}
			if "method" in hook_entry and isinstance(hook_entry["method"], str):
				method = hook_entry["method"]
				defined_methods.append(method)
				insert_single_event(method, hook_entry)
			else:
				# Case 2: {"path": {"limit": 10}} OR {"category": ["m1", "m2"]}
				for key, value in hook_entry.items():
					if isinstance(value, dict):
						# Method is key, value is config
						defined_methods.append(key)
						insert_single_event(key, value)
					elif isinstance(value, list):
						# Key is category, value is list of methods
						for method in value:
							if isinstance(method, str):
								defined_methods.append(method)
								insert_single_event(method)
							elif isinstance(method, dict) and "method" in method:
								m_name = method["method"]
								defined_methods.append(m_name)
								insert_single_event(m_name, method)

	# Clear old ones
	for job in frappe.get_all("Controller Job Type", fields=["name", "method", "server_script"]):
		if not job.server_script and job.method not in defined_methods:
			# Instead of deleting (which fails if logs/jobs exist), we just stop them
			frappe.db.set_value("Controller Job Type", job.name, "stopped", 1)

def insert_single_event(method: str, config: dict = None):
	if not method:
		return

	config = config or {}
	job_name = frappe.db.exists("Controller Job Type", {"method": method})
	
	if job_name:
		doc = frappe.get_doc("Controller Job Type", job_name)
	else:
		doc = frappe.new_doc("Controller Job Type")
		doc.method = method
		doc.stopped = 0
		doc.create_log = 1

	# Safely update numeric fields
	if config.get("max_calls_per_minute") is not None:
		try:
			doc.max_calls_per_minute = int(config.get("max_calls_per_minute"))
		except (ValueError, TypeError):
			pass
			
	if config.get("concurrency_limit") is not None:
		try:
			doc.concurrency_limit = int(config.get("concurrency_limit"))
		except (ValueError, TypeError):
			pass
	
	doc.save(ignore_permissions=True)
