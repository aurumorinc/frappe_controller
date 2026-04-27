# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import os
import time
import random
import json
from typing import NoReturn
from filelock import FileLock, Timeout

import frappe
from frappe.utils import get_bench_path, get_sites, now_datetime
from frappe.utils.background_jobs import set_niceness

DEFAULT_CONTROLLER_TICK = 1

def start_controller() -> NoReturn:
	"""Run enqueue_jobs_for_all_sites based on controller tick."""

	tick = DEFAULT_CONTROLLER_TICK
	set_niceness()

	lock_path = _get_controller_lock_file()

	try:
		lock = FileLock(lock_path)
		lock.acquire(blocking=False)
	except Timeout:
		frappe.logger("controller").debug("Controller already running")
		return

	while True:
		# Use a precise sleep to maintain 1s tick
		start_time = time.time()
		enqueue_jobs_for_all_sites()
		elapsed = time.time() - start_time
		sleep_time = max(0.1, tick - elapsed)
		time.sleep(sleep_time)

def _get_controller_lock_file():
	return os.path.abspath(os.path.join(get_bench_path(), "config", "controller_process"))

def enqueue_jobs_for_all_sites():
	"""Loop through sites and enqueue controller jobs"""

	with frappe.init_site():
		sites = get_sites()

	random.shuffle(sites)

	for site in sites:
		try:
			enqueue_jobs_for_site(site=site)
		except Exception:
			frappe.logger("controller").debug(f"Failed to enqueue jobs for site: {site}", exc_info=True)

def enqueue_jobs_for_site(site: str):
	try:
		frappe.init(site)
		frappe.connect()
		
		if frappe.local.conf.maintenance_mode or frappe.local.conf.pause_controller:
			return

		enqueue_jobs()

	except Exception as e:
		frappe.logger("controller").error(f"Exception in Enqueue Jobs for Site {site}", exc_info=True)
	finally:
		frappe.destroy()

def enqueue_jobs():
	"""
	The Hatchet-style Dispatcher.
	Pulls Queued Controller Jobs from DB and pushes them to Redis workers.
	"""
	# Get all active job types
	job_types = frappe.get_all("Controller Job Type", filters={"stopped": 0}, fields=["*"])
	
	for jt in job_types:
		# Check concurrency limit
		running_count = frappe.db.count("Controller Job", {
			"job_type": jt.name, 
			"status": "Started"
		})
		concurrency_limit = jt.concurrency_limit or 1
		
		if running_count >= concurrency_limit:
			continue
			
		slots = concurrency_limit - running_count
		
		# Pull Queued tasks from DB
		queued_tasks = frappe.get_all("Controller Job", filters={
			"job_type": jt.name,
			"status": "Queued"
		}, fields=["name", "job_name", "arguments", "queue", "timeout"], 
		order_by="creation ASC", limit=slots)
		
		if not queued_tasks:
			continue

		jt_doc = frappe.get_doc("Controller Job Type", jt.name)

		for task in queued_tasks:
			# Check rate limit (calls per minute)
			if not jt_doc.is_allowed_by_rate_limit():
				break
				
			# Dispatch to real background worker
			frappe.enqueue(
				method="frappe_controller.utils.controller.run_job",
				queue=task.queue or "default",
				timeout=task.timeout,
				is_async=True,
				task_name=task.name
			)
			
			# Mark as Started in DB
			frappe.db.set_value("Controller Job", task.name, {
				"status": "Started",
				"started_at": now_datetime()
			}, update_modified=False)
			
		frappe.db.commit()

def run_job(task_name):
    """
    Wrapper function that runs in the standard RQ worker.
    It executes the payload and updates the Controller Job status.
    """
    if not frappe.db.exists("Controller Job", task_name):
        return

    job = frappe.get_doc("Controller Job", task_name)
    
    try:
        # 1. Prepare execution
        method_path = job.job_name
        args = json.loads(job.arguments) if job.arguments else {}
        
        # 2. Execute target function
        frappe.get_attr(method_path)(**args)
        
        # 3. Success
        job.status = "Finished"
        
    except Exception:
        # 4. Failure
        frappe.db.rollback()
        job.status = "Failed"
        job.exc_info = frappe.get_traceback()
        
    finally:
        # 5. Cleanup and State sync
        job.ended_at = now_datetime()
        if job.started_at:
            job.time_taken = (job.ended_at - job.started_at).total_seconds()
        
        job.save(ignore_permissions=True)
        frappe.db.commit()
