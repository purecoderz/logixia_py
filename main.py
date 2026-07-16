import httpx
from fastapi import HTTPException
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

class CoachRequest(BaseModel):
    userCode: str
    taskInstructions: str
    chatHistory: List[Dict[str, Any]]
    tests: List[Dict[str, Any]]  # 👈 Added to pass the test suite payload
    
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

# --- 4. REST API: SOCRATIC AI COACH ---
# ==========================================
# HARD BACKEND REGEX GUARDRAIL
# ==========================================
# --- 4. REST API: SOCRATIC AI COACH ---
@app.post("/api/coach")
async def ask_coach(payload: CoachRequest):
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY is not configured on the server.")

    # Strict Socratic instructions
    system_prompt = (
        "You are the Logixia AI Logic Coach. Your core philosophy is: 'Master the logic. The syntax will follow.'\n"
        "CRITICAL MANDATE: You are strictly forbidden from writing code, fixing the user's syntax, or providing solutions.\n"
        "Guidelines:\n"
        "1. Analyze the user's code against the task instructions.\n"
        "2. If there is an error, point them to the exact line or section to inspect.\n"
        "3. Turn their question back into a targeted Socratic question that helps them spot their logical mistake."
    )

    # Build the messages payload chronologically
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user", 
            "content": f"Context for this session:\nTask Instructions:\n{payload.taskInstructions}\n\nMy current code:\n{payload.userCode}\n\nPlease help me step-by-step."
        }
    ]
    
    # Append the running chat history from the React frontend
    messages.extend(payload.chatHistory)

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_api_key}"},
            json={
                "model": "llama-3.3-70b-specdec", # Groq's fast, free reasoning model
                "temperature": 0.3,
                "messages": messages
            },
            timeout=30.0
        )
    
    if response.status_code != 200:
        error_details = response.json().get("error", {}).get("message", "Unknown error from Groq")
        raise HTTPException(status_code=response.status_code, detail=f"Groq API error: {error_details}")

    data = response.json()
    return {"guidance": data["choices"][0]["message"]["content"]}
    
async def ask_coach(payload: CoachRequest):
    groq_api_key = os.getenv("GROQ_API_KEY")
    if not groq_api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY is not configured on the server.")

    # Socratic Tough-Love instructions with direct exposure to test assertions
    system_prompt = (
        "You are the Logixia AI Logic Coach. Your core philosophy is: 'Master the logic. The syntax will follow.'\n"
        "CRITICAL MANDATE: You are strictly forbidden from writing code, fixing the user's syntax, providing refactored snippets, or handing out direct solutions.\n"
        "Do NOT use markdown code blocks (triple backticks) under any circumstances.\n\n"
        "Guidelines based on user progress:\n"
        "1. IF THE CODE IS EMPTY OR SHOWS ZERO EFFORT: Call them out on it directly! Tell them you can see they haven't even tried yet. "
        "Refuse to give them hints or discuss the tests until they write down a plan or attempt some code. Make them do the mental heavy lifting.\n"
        "2. IF THE USER HAS A REASONABLE ATTEMPT BUT TESTS ARE FAILING: Analyze their code specifically against the provided Test Suite specifications (inputs, expected outputs, unit test assertions). "
        "Do not tell them the exact values that failed, but identify which test logic block/rule they are violating and point to the exact line/section in their code causing the failure.\n"
        "3. IF THE USER'S CODE PASSES ALL TESTS: Congratulate them briefly, then pivot immediately to code quality! "
        "Analyze their working code for structural or theoretical improvements—such as time/space complexity (Big O), readability, redundantly nested loops, or language-specific idioms (e.g., Pythonic code list comprehensions, built-in functions). "
        "Socratically guide them to identify these refactoring opportunities themselves by asking them how they might optimize their working solution.\n"
        "4. ALWAYS turn their explicit question back into a targeted Socratic counter-question that forces them to discover their own logical mistakes or architectural improvements."
    )

    # Format the tests into a readable JSON block for the LLM context
    formatted_tests = json.dumps(payload.tests, indent=2)

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user", 
            "content": (
                f"Context for this session:\n"
                f"Task Instructions:\n{payload.taskInstructions}\n\n"
                f"Validation Test Suite (Rules I must pass):\n{formatted_tests}\n\n"
                f"My current code:\n{payload.userCode}\n\n"
                f"Please help me step-by-step."
            )
        }
    ]
    
    # Safely unpack history, passing only standard roles and content values to Groq
    for msg in payload.chatHistory:
        role = msg.get("role")
        content = msg.get("content")
        if role and content:
            messages.append({"role": str(role), "content": str(content)})

    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_api_key}"},
            json={
                "model": "llama-3.3-70b-versatile",
                "temperature": 0.2,
                "messages": messages
            },
            timeout=30.0
        )
    
    if response.status_code != 200:
        error_details = response.json().get("error", {}).get("message", "Unknown error from Groq")
        raise HTTPException(status_code=response.status_code, detail=f"Groq API error: {error_details}")

    data = response.json()
    raw_content = data["choices"][0]["message"]["content"]
    
    # Pass the raw completion through the Socratic check
    final_content = enforce_socratic_guardrail(raw_content)
    
    return {"guidance": final_content}
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
