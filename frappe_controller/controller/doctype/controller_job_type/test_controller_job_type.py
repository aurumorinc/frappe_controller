# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import frappe
from frappe.tests import IntegrationTestCase
from frappe_controller.controller.doctype.controller_job_type.controller_job_type import sync_jobs

class TestControllerJobType(IntegrationTestCase):
	def setUp(self):
		frappe.db.rollback()
		frappe.db.truncate("Controller Job Type")
		frappe.db.truncate("Controller Job")

	def test_sync_jobs_from_hooks(self):
		test_hooks = {
			"method.one": {"max_calls_per_minute": 10},
			"method.two": {"concurrency_limit": 2}
		}
		
		sync_jobs(hooks=test_hooks)

		self.assertTrue(frappe.db.exists("Controller Job Type", {"method": "method.one"}))
		self.assertTrue(frappe.db.exists("Controller Job Type", {"method": "method.two"}))

		job1 = frappe.get_doc("Controller Job Type", {"method": "method.one"})
		self.assertEqual(job1.max_calls_per_minute, 10)

		# Test cleanup of orphaned jobs
		sync_jobs(hooks={"method.one": {}})
		self.assertFalse(frappe.db.exists("Controller Job Type", {"method": "method.two"}))

	def test_rate_limiting_logic(self):
		job = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "test.rate.limit",
			"max_calls_per_minute": 2,
			"stopped": 0
		}).insert()

		# Ensure cache is clean
		cache_key = f"controller_job_rate_limit:{job.name}"
		frappe.cache().delete(cache_key)

		# First two calls should be allowed
		self.assertTrue(job.is_allowed_by_rate_limit())
		self.assertTrue(job.is_allowed_by_rate_limit())

		# Third call should be blocked
		self.assertFalse(job.is_allowed_by_rate_limit())
