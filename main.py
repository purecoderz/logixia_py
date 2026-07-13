import asyncio
import os
import json
import tempfile
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState
from pydantic import BaseModel
from typing import List, Dict, Any

app = FastAPI(title="Logixia Interactive Execution Engine")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- MODELS ---
class SubmitPayload(BaseModel):
    code: str
    tests: List[Dict[str, Any]]

# --- SESSION TRACKER ---
class ActiveSession:
    def __init__(self):
        self.process = None
        self.task = None

    def cancel(self):
        """Cleanly tears down both the async task and the OS process."""
        if self.task and not self.task.done():
            self.task.cancel()
        if self.process:
            try:
                self.process.kill()
            except ProcessLookupError:
                pass
        self.process = None
        self.task = None

# --- STREAM READER (For Interactive Mode) ---
async def stream_reader(stream, stream_type: str, websocket: WebSocket):
    try:
        while True:
            line = await stream.read(1024)
            if not line:
                break
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "event": "output",
                    "stream": stream_type,
                    "data": line.decode('utf-8', errors='replace')
                })
    except asyncio.CancelledError:
        pass

# --- 1. NORMAL RUN (Freeplay / Interactive Console) ---
async def run_interactive_code(code: str, session: ActiveSession, websocket: WebSocket, timeout_seconds: float = 30.0):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_script:
        temp_script.write(code)
        temp_script_path = temp_script.name

    try:
        session.process = await asyncio.create_subprocess_exec(
            'python3', '-u', temp_script_path,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        tasks = [
            asyncio.create_task(stream_reader(session.process.stdout, "stdout", websocket)),
            asyncio.create_task(stream_reader(session.process.stderr, "stderr", websocket))
        ]

        try:
            return_code = await asyncio.wait_for(session.process.wait(), timeout=timeout_seconds)
            for t in tasks: t.cancel()
            
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({"event": "exit", "return_code": return_code})
                
        except asyncio.TimeoutError:
            session.cancel()
            if websocket.client_state == WebSocketState.CONNECTED:
                await websocket.send_json({
                    "event": "output",
                    "stream": "stderr",
                    "data": f"\n❌ TimeLimitError: Session timed out after {timeout_seconds} seconds.\n"
                })
                await websocket.send_json({"event": "exit", "return_code": 124})

    except asyncio.CancelledError:
        pass
    except Exception as e:
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.send_json({"event": "output", "stream": "stderr", "data": f"\nSystem Error: {str(e)}\n"})
    finally:
        if os.path.exists(temp_script_path):
            os.remove(temp_script_path)

# --- get health
@app.get("/health")
async def health_check():
    return {"status": "awake"}

# --- 2. REST API: VALIDATION SUBMISSION (FIXED) ---
@app.post("/api/submit")
async def submit_code_http(payload: SubmitPayload):
    code = payload.code
    test_cases = payload.tests

    for test in test_cases:
        test_type = test.get("type")
        test_id = test.get("id")

        valid_types = ["unit_test", "io_match", "syntax_check"]
        if test_type not in valid_types:
            return {
                "status": "error",
                "id": test_id,
                "error_message": f"SYSTEM ERROR: Unrecognized test type '{test_type}'.",
                "feedback": "Please contact support."
            }
        
        execution_code = code
        if test_type == "unit_test":
            # FIXED: Looks for "test_code" to match your JSON payload
            execution_code += "\n\n# --- HIDDEN TESTS ---\n" + test.get("test_code", "")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_script:
            temp_script.write(execution_code)
            temp_script_path = temp_script.name

        try:
            process = await asyncio.create_subprocess_exec(
                'python3', '-u', temp_script_path,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # FIXED: Inject inputs for ANY test type if they are provided
            inputs = test.get("injected_inputs", [])
            if inputs and process.stdin:
                input_string = "\n".join(inputs) + "\n"
                process.stdin.write(input_string.encode('utf-8'))
                await process.stdin.drain()
            
            if process.stdin:
                process.stdin.close()

            stdout_data, stderr_data = await asyncio.wait_for(process.communicate(), timeout=5.0)
            
            stdout_str = stdout_data.decode('utf-8').strip()
            stderr_str = stderr_data.decode('utf-8').strip()
            return_code = process.returncode

            status = "failed"
            if return_code != 0:
                status = "error"
            elif test_type == "unit_test":
                status = "passed"
            elif test_type == "io_match":
                expected = test.get("expected_output", "").strip()
                match_type = test.get("match_type", "exact")
                if match_type == "exact" and stdout_str == expected:
                    status = "passed"
                elif match_type == "contains" and expected in stdout_str:
                    status = "passed"

            if status in ["failed", "error"]:
                return {
                    "status": status,
                    "id": test_id,
                    "error_message": stderr_str if stderr_str else f"Output did not match expected results. Got: {stdout_str}",
                    "feedback": test.get("feedback_message")
                }

        except asyncio.TimeoutError:
            return {
                "status": "error",
                "id": test_id,
                "error_message": "Execution timed out.",
                "feedback": test.get("feedback_message")
            }
        finally:
            if os.path.exists(temp_script_path):
                os.remove(temp_script_path)
            try:
                process.kill()
            except:
                pass

    return {"status": "passed"}
    
# --- 3. WEBSOCKET ENDPOINT: INTERACTIVE CONSOLE ---
@app.websocket("/ws/execute")
async def websocket_endpoint(websocket: WebSocket):
    """
    Handles live, bidirectional interactive execution streams.
    """
    await websocket.accept()
    session = ActiveSession()
    print("🚀 Client connected for interactive session")
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            payload = json.loads(raw_data)
            action = payload.get("action")
            
            if action == "run":
                # Triggers the live interactive console mode
                code = payload.get("code", "")
                session.cancel()
                if websocket.client_state == WebSocketState.CONNECTED:
                    await websocket.send_json({"event": "status", "data": "Spawning interactive environment...\n"})
                
                session.task = asyncio.create_task(
                    run_interactive_code(code, session, websocket)
                )
                
            elif action == "input":
                # Pipes user keystrokes into the active process
                input_data = payload.get("data", "")
                if session.process and session.process.stdin:
                    session.process.stdin.write(input_data.encode('utf-8'))
                    await session.process.stdin.drain()
                    
    except WebSocketDisconnect:
        print("🔌 Client disconnected smoothly")
    finally:
        session.cancel()
