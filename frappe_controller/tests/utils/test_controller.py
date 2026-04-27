# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import frappe
import json
import time
from unittest.mock import patch
from frappe.tests import IntegrationTestCase
from frappe.utils import now_datetime
from frappe_controller.utils.background_jobs import enqueue
from frappe_controller.utils.controller import enqueue_jobs, run_job

class TestControllerDaemon(IntegrationTestCase):
	def setUp(self):
		frappe.db.rollback()
		frappe.db.truncate("Controller Job Type")
		frappe.db.truncate("Controller Job")
		
		# Create a job type for testing
		self.job_type = frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe.ping",
			"max_calls_per_minute": 100,
			"concurrency_limit": 5,
			"stopped": 0
		}).insert()
		frappe.db.commit()

	def test_custom_enqueue_creates_record(self):
		job_name = enqueue("frappe.ping", some_arg="value")
		
		self.assertTrue(frappe.db.exists("Controller Job", job_name))
		job = frappe.get_doc("Controller Job", job_name)
		self.assertEqual(job.status, "Queued")
		self.assertEqual(json.loads(job.arguments).get("some_arg"), "value")

	def test_dispatcher_respects_concurrency(self):
		# Create 10 queued jobs for this specific job type
		for _ in range(10):
			enqueue("frappe.ping")
		
		# Ensure they are in the DB and linked correctly
		self.assertEqual(frappe.db.count("Controller Job", {"job_type": self.job_type.name, "status": "Queued"}), 10)

		# Simulate 3 jobs already running
		queued_jobs = frappe.get_all("Controller Job", filters={"status": "Queued", "job_type": self.job_type.name}, limit=3)
		for j in queued_jobs:
			frappe.db.set_value("Controller Job", j.name, "status", "Started")
		
		frappe.db.commit()

		# Now 3 are Started, 7 are Queued. Limit is 5.
		# Dispatcher should only pick up 2 more (Total limit 5 - 3 running = 2 slots)
		with patch("frappe.enqueue") as mock_rq_enqueue:
			enqueue_jobs()
			self.assertEqual(mock_rq_enqueue.call_count, 2)

	def test_run_job_wrapper_updates_status(self):
		job_name = enqueue("frappe.ping")
		# Manually move to Started to simulate being picked up by dispatcher
		frappe.db.set_value("Controller Job", job_name, {
			"status": "Started",
			"started_at": now_datetime()
		})
		frappe.db.commit()

		run_job(job_name)

		job = frappe.get_doc("Controller Job", job_name)
		self.assertEqual(job.status, "Finished")
		self.assertIsNotNone(job.ended_at)
		self.assertGreaterEqual(job.time_taken, 0)

	def test_run_job_wrapper_handles_failure(self):
		# Enqueue a non-existent method to force failure
		job_name = enqueue("non_existent_method_xyz")
		frappe.db.set_value("Controller Job", job_name, {
			"status": "Started",
			"started_at": now_datetime()
		})
		frappe.db.commit()

		run_job(job_name)

		job = frappe.get_doc("Controller Job", job_name)
		self.assertEqual(job.status, "Failed")
		self.assertIsNotNone(job.exc_info)
		self.assertTrue("Error" in job.exc_info)
