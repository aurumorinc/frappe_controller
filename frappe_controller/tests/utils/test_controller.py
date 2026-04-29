# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import frappe
import json
import time
from unittest.mock import patch, MagicMock
from frappe.tests import IntegrationTestCase
from frappe.utils import now_datetime, add_to_date
from frappe_controller.utils.background_jobs import enqueue
from frappe_controller.utils.controller import enqueue_jobs, run_job, cleanup_zombie_jobs

class TestControllerDaemon(IntegrationTestCase):
	def setUp(self):
		frappe.db.rollback()
		frappe.db.truncate("Controller Job Type")
		frappe.db.truncate("Controller Job")
		frappe.db.truncate("Controller Job Log")
		
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
			class MockJob:
				def __init__(self):
					import uuid
					self.id = str(uuid.uuid4())
			
			mock_rq_enqueue.side_effect = lambda **kw: MockJob()

			enqueue_jobs()
			self.assertEqual(mock_rq_enqueue.call_count, 2)
			
			# Verify that the jobs picked up got marked as Started and assigned job_id
			started_jobs_count = frappe.db.count("Controller Job", {"status": "Started", "job_type": self.job_type.name})
			self.assertEqual(started_jobs_count, 5)

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
		
		# Verify log creation for successful job
		logs = frappe.get_all("Controller Job Log", filters={"controller_job_type": job.job_type, "status": "Complete"})
		self.assertGreaterEqual(len(logs), 1)
		self.assertIn("successfully", frappe.db.get_value("Controller Job Log", logs[0].name, "details"))

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
		self.assertTrue("Error" in job.exc_info or "AttributeError" in job.exc_info)
		
		# Verify log creation for failed job
		logs = frappe.get_all("Controller Job Log", filters={"controller_job_type": job.job_type, "status": "Failed"})
		self.assertGreaterEqual(len(logs), 1)
		self.assertTrue("AttributeError" in frappe.db.get_value("Controller Job Log", logs[0].name, "details") or "Error" in frappe.db.get_value("Controller Job Log", logs[0].name, "details"))

	def test_opt_out_logging(self):
		# Set create_log to 0
		frappe.db.set_value("Controller Job Type", self.job_type.name, "create_log", 0)
		
		job_name = enqueue("frappe.ping")
		frappe.db.set_value("Controller Job", job_name, {
			"status": "Started",
			"started_at": now_datetime()
		})
		frappe.db.commit()

		run_job(job_name)
		
		# Assert 0 logs generated for this execution cycle
		logs = frappe.get_all("Controller Job Log", filters={"controller_job_type": self.job_type.name})
		self.assertEqual(len(logs), 0)
		
		frappe.db.set_value("Controller Job Type", self.job_type.name, "create_log", 1) # reset

	def test_cleanup_zombie_jobs(self):
		# 1. Healthy Job (started 10 seconds ago, timeout is 60s)
		job1_name = enqueue("frappe.ping", timeout=60)
		frappe.db.set_value("Controller Job", job1_name, {
			"status": "Started",
			"started_at": add_to_date(now_datetime(), seconds=-10),
			"timeout": 60
		})

		# 2. Zombie Job (started 200 seconds ago, timeout is 60s) -> should be failed
		job2_name = enqueue("frappe.ping", timeout=60)
		frappe.db.set_value("Controller Job", job2_name, {
			"status": "Started",
			"started_at": add_to_date(now_datetime(), seconds=-200),
			"timeout": 60
		})

		# 3. Zombie Job without timeout set (defaults to 3600 + 60s) -> started 4000s ago
		job3_name = enqueue("frappe.ping")
		frappe.db.set_value("Controller Job", job3_name, {
			"status": "Started",
			"started_at": add_to_date(now_datetime(), seconds=-4000),
			"timeout": 0 # or None
		})
		
		frappe.db.commit()

		cleanup_zombie_jobs()

		self.assertEqual(frappe.db.get_value("Controller Job", job1_name, "status"), "Started")
		self.assertEqual(frappe.db.get_value("Controller Job", job2_name, "status"), "Failed")
		self.assertEqual(frappe.db.get_value("Controller Job", job3_name, "status"), "Failed")

		# Test Zombie Job Log generation
		logs = frappe.get_all("Controller Job Log", filters={"controller_job_type": self.job_type.name, "status": "Failed"})
		self.assertGreaterEqual(len(logs), 2)
		self.assertIn("Worker crashed", frappe.db.get_value("Controller Job Log", logs[0].name, "details"))

	def test_consolidate_queries_and_batch_update_efficiency(self):
		frappe.db.truncate("Controller Job")
		
		# Create a second job type
		frappe.get_doc({
			"doctype": "Controller Job Type",
			"method": "frappe.get_all",
			"concurrency_limit": 10,
			"stopped": 0
		}).insert()

		for _ in range(5):
			enqueue("frappe.ping")
		for _ in range(3):
			enqueue("frappe.get_all")
		frappe.db.commit()

		# Run enqueue_jobs and track db.sql calls
		original_db_sql = frappe.db.sql
		with patch.object(frappe.db, "sql", side_effect=original_db_sql) as mock_sql:
			with patch("frappe.enqueue") as mock_rq_enqueue:
				class MockJob:
					def __init__(self):
						import uuid
						self.id = str(uuid.uuid4())
				
				mock_rq_enqueue.side_effect = lambda **kw: MockJob()

				enqueue_jobs()

				self.assertEqual(mock_rq_enqueue.call_count, 8)
				
				# We expect frappe.db.sql to be called:
				# 1 time for SELECT GROUP BY running count
				# 1 time for the batch UPDATE 
				# (Plus possibly a few internal Frappe queries, but the core loops shouldn't N+1)
				sql_calls = mock_sql.call_args_list
				
				select_group_by_called = False
				batch_update_called = False

				for call in sql_calls:
					query = call[0][0]
					if "GROUP BY job_type" in query:
						select_group_by_called = True
					if "UPDATE `tabController Job`" in query and "job_id = %s" in query:
						batch_update_called = True
						
				self.assertTrue(select_group_by_called, "Missing consolidated GROUP BY query")
				self.assertTrue(batch_update_called, "Missing batch UPDATE query")
				
				started_count = frappe.db.count("Controller Job", {"status": "Started"})
				self.assertEqual(started_count, 8)
