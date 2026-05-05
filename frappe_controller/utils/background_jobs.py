# Copyright (c) 2026, Aurumor and contributors
# License: MIT. See LICENSE

import json
import frappe
from frappe.utils import now_datetime

def enqueue(method, queue="low", timeout=None, is_async=True, **kwargs):
	"""
	Replacement for frappe.enqueue. 
	Instead of enqueuing directly to Redis, it creates a Controller Job record in MariaDB.
	"""
	if queue not in ("low", "medium", "high"):
		import frappe.utils.background_jobs as native_bg
		return native_bg.enqueue(method, queue=queue, timeout=timeout, is_async=is_async, **kwargs)

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
		"doctype": "FS Job",
		"job_type": job_type_name,
		"job_name": method,
		"queue": queue,
		"status": "Queued",
		"arguments": json.dumps(kwargs, default=str)
	})
	job.insert(ignore_permissions=True)
	job.db_set("job_id", job.name)
	job.job_id = job.name
	
	job_payload = job.as_dict()
	job_payload["site"] = frappe.local.site

	# If site is initialized, commit so the daemon can see it immediately
	if frappe.db:
		frappe.db.commit()
		# Push to Redis Stream for FastStream
		frappe.cache().xadd(f"fs:queue:{queue}", {"payload": json.dumps(job_payload, default=str)})

		
	return job.name


import asyncio
import time
from typing import Dict, Any

import redis.asyncio as aioredis
from faststream import FastStream
from faststream.redis import RedisBroker, StreamSub
import anyio



def start_worker(queue="default"):
    """
    Programmatic entry point to start the FastStream worker for a specific queue.
    """
    import frappe
    if not getattr(frappe.local, "site", None):
        frappe.init(frappe.utils.get_sites()[0])
    redis_url = frappe.conf.get("redis_cache") or "redis://localhost:13000"

    import redis
    sync_redis = redis.Redis.from_url(redis_url)
    INGESTION_STREAM = f"fs:queue:{queue}"
    try:
        sync_redis.xgroup_create(INGESTION_STREAM, "faststream_workers", id="0", mkstream=True)
    except Exception:
        pass
    finally:
        sync_redis.close()

    redis_client = aioredis.from_url(redis_url)
    broker = RedisBroker(url=redis_url)
    app = FastStream(broker)
    
    INGESTION_STREAM = f"fs:queue:{queue}"
    DELAYED_JOBS_ZSET = f"fs:scheduled:{queue}"
    FINISHED_STREAM = f"fs:finished:{queue}"
    FAILED_STREAM = f"fs:failed:{queue}"

    async def check_rate_limits(method: str) -> float:
        lua_script = """
        local method = KEYS[1]
        local current_time = tonumber(ARGV[1])
        
        local config_key = "fs:" .. method .. ":config"
        local limits = redis.call('HGETALL', config_key)
        
        if #limits == 0 then
            return 0 -- no limits found
        end
        
        local config = {}
        for i=1, #limits, 2 do
            config[limits[i]] = tonumber(limits[i+1])
        end
        
        local keys = {
            min = "fs:" .. method .. ":rate:1m",
            hour = "fs:" .. method .. ":rate:1h",
            day = "fs:" .. method .. ":rate:1d"
        }
        
        local windows = {
            min = 60,
            hour = 3600,
            day = 86400
        }
        
        -- Check all limits first
        if config['rate_limit_per_minute'] and tonumber(redis.call('GET', keys.min) or 0) >= config['rate_limit_per_minute'] then
            return current_time + windows.min
        end
        if config['rate_limit_per_hour'] and tonumber(redis.call('GET', keys.hour) or 0) >= config['rate_limit_per_hour'] then
            return current_time + windows.hour
        end
        if config['rate_limit_per_day'] and tonumber(redis.call('GET', keys.day) or 0) >= config['rate_limit_per_day'] then
            return current_time + windows.day
        end
        
        -- If allowed, increment
        if config['rate_limit_per_minute'] then
            local count = redis.call('INCR', keys.min)
            if count == 1 then redis.call('EXPIRE', keys.min, windows.min) end
        end
        if config['rate_limit_per_hour'] then
            local count = redis.call('INCR', keys.hour)
            if count == 1 then redis.call('EXPIRE', keys.hour, windows.hour) end
        end
        if config['rate_limit_per_day'] then
            local count = redis.call('INCR', keys.day)
            if count == 1 then redis.call('EXPIRE', keys.day, windows.day) end
        end
        
        return 0
        """
        delay_until = await redis_client.eval(lua_script, 1, method, time.time())
        return delay_until

    async def handle_ingestion(msg: Dict[str, Any]):
        try:
            payload_str = msg.get("payload")
            if not payload_str:
                return
                
            if isinstance(payload_str, bytes):
                payload_str = payload_str.decode()
                
            import json
            try:
                payload = json.loads(payload_str)
            except Exception as e:
                return
                
            job_id = payload.get("name")
            if not job_id:
                return
                
            try:
                method_path = payload.get("job_name")
                args_str = payload.get("arguments")
                
                lock_key = f"fs:started:{job_id}"
                is_locked = await redis_client.setnx(lock_key, "1")
                if not is_locked:
                    return

                await redis_client.expire(lock_key, 3660)
                
                args = json.loads(args_str) if args_str else {}
                
                delay_until = await check_rate_limits(method_path)
                
                if delay_until > 0:
                    await redis_client.zadd(DELAYED_JOBS_ZSET, {json.dumps(msg): delay_until})
                    await redis_client.delete(lock_key)
                    return
                    
                start_time = time.time()
                
                site_name = payload.get("site")
                if not site_name:
                    site_name = getattr(frappe.local, "site", None) or frappe.utils.get_sites()[0]
                
                STARTED_STREAM = f"fs:started:{queue}"
                await redis_client.xadd(STARTED_STREAM, {
                    "payload": json.dumps({
                        "job_id": job_id,
                        "status": "Started",
                        "started_at": str(frappe.utils.now_datetime()),
                        "site": site_name
                    }, default=str)
                })

                async def run_frappe():
                    def execute():
                        frappe.init(site=site_name, force=True)
                        frappe.connect()
                        try:
                            func = frappe.get_attr(method_path)
                            func(**args)
                            frappe.db.commit()
                        except Exception:
                            frappe.db.rollback()
                            raise
                        finally:
                            frappe.destroy()
                    
                    await anyio.to_thread.run_sync(execute)
                        
                error = None
                status = "Finished"
                
                try:
                    await run_frappe()
                except Exception as e:
                    status = "Failed"
                    error = str(e)
                    
                time_taken = time.time() - start_time
                
                telemetry_stream = FINISHED_STREAM if status == "Finished" else FAILED_STREAM
                await redis_client.xadd(telemetry_stream, {
                    "payload": json.dumps({
                        "job_id": job_id,
                        "status": status,
                        "error": error,
                        "time_taken": time_taken,
                        "site": site_name
                    }, default=str)
                })
                
            finally:
                if getattr(frappe.local, "site", None):
                    frappe.db.rollback()
        except Exception as outer_e:
            raise

    @app.on_startup
    async def init_streams_and_promoter():
        async def promote():
            while True:
                current_time = time.time()
                try:
                    jobs = await redis_client.zrangebyscore(DELAYED_JOBS_ZSET, "-inf", current_time)
                    if jobs:
                        for job_str in jobs:
                            job_data = json.loads(job_str)
                            await broker.publish(job_data, stream=INGESTION_STREAM)
                        await redis_client.zremrangebyscore(DELAYED_JOBS_ZSET, "-inf", current_time)
                except Exception:
                    pass
                await asyncio.sleep(1)

        asyncio.create_task(promote())

    # Dynamically bind the subscriber
    broker.subscriber(stream=StreamSub(INGESTION_STREAM, group="faststream_workers", consumer="consumer-1"))(handle_ingestion)

    # Run the application
    anyio.run(app.run)
