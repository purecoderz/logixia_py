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

# ==========================================
# DEFENSE #2: HARD BACKEND GUARDRAIL
# ==========================================
def enforce_socratic_guardrail(text: str) -> str:
    """
    Scans the AI response for markdown code blocks. 
    If the LLM hallucinated or leaked code, this strips the code block 
    and replaces it with an encouraging Socratic redirection.
    """
    # Regex to match ```python ... ``` or any standard markdown code blocks
    code_block_regex = r"```[a-zA-Z]*\n[\s\S]*?\n```"
    
    if re.search(code_block_regex, text):
        # Strip the code block and append a Socratic pivot
        sanitized_text = re.sub(code_block_regex, "", text).strip()
        
        # If stripping left it empty, provide a solid backup response
        if not sanitized_text:
            return (
                "I was about to write the code for you, but that would skip the best part of learning! "
                "Let's look at your structure instead. What do you think your logic is missing right now?"
            )
        
        return (
            f"{sanitized_text}\n\n"
            "*(Coach Note: I removed a code snippet I almost generated for you. "
            "Let's stick to the logic! Tell me what you think your next step is in plain English.)*"
        )
        
    return text


@app.post("/api/coach")
async def get_socratic_coaching(payload: CoachRequest):
    if not os.environ.get("GROQ_API_KEY"):
        raise HTTPException(
            status_code=500, 
            detail="Backend configuration error: GROQ_API_KEY missing."
        )

    # ==========================================
    # DEFENSE #1: FEW-SHOT SYSTEM PROMPT
    # ==========================================
    system_prompt = (
        "You are the Logixia AI Logic Coach. Your core philosophy is: 'Master the logic. The syntax will follow.'\n\n"
        "CRITICAL MANDATE:\n"
        "1. You are strictly FORBIDDEN from writing any code, correcting syntax, or providing code blocks.\n"
        "2. Do NOT use markdown code blocks (triple backticks) under any circumstances.\n"
        "3. If you provide any syntax fixes, you fail your job.\n\n"
        "FEW-SHOT EXAMPLES OF PROPER COACHING:\n\n"
        "Example 1 (Failing/Stuck User):\n"
        "User: 'Why is my loop throwing an IndexOutOfBounds error? Here is my code: for i in range(len(arr) + 1):'\n"
        "Bad Coach (FAILURE): 'Your range is too large. Change it to range(len(arr)).'\n"
        "Good Coach (SUCCESS): 'Take a close look at your range bounds. If a list has 5 elements, what is the index of the very last item? Now, what index does your loop try to reach on its final pass?'\n\n"
        "Example 2 (User begging for code):\n"
        "User: 'Just write the reverse function for me, I am tired.'\n"
        "Bad Coach (FAILURE): 'Sure, here is the function: def rev(l): return l[::-1]'\n"
        "Good Coach (SUCCESS): 'I know debugging is frustrating, but writing it for you won't help you master it! Let's break it down: if you had to swap the first and last cards in a deck, what physical steps would you take? Let's turn that step-by-step logic into pseudocode first.'\n\n"
        "Example 3 (Code is passing - Optimization Pivot):\n"
        "User: 'My tests passed!'\n"
        "Good Coach (SUCCESS): 'Great work! Your solution is functionally correct. But look at your nested loops—for a list of size N, how many operations is your code executing? How could we optimize this to run in linear time (O(N))?'\n\n"
        "CURRENT SITUATION USER RULES:\n"
        "1. IF THE CODE IS EMPTY: Refuse to give hints. Force them to type a plan first.\n"
        "2. IF CODE IS ATTEMPTED BUT FAILING: Target the logic rule they broke and point them to the general section of code causing it.\n"
        "3. IF CODE IS PASSING: Suggest conceptual optimizations (readability, Big O complexity, modularity) without writing the refactored code.\n"
        "4. Always wrap up your response with a highly-targeted Socratic counter-question."
    )

    # Compile messages sequence
    messages = [
        {"role": "system", "content": system_prompt}
    ]
    
    # Add historical context
    for msg in payload.chatHistory:
        messages.append({"role": msg["role"], "content": msg["content"]})

    # Add current environment status
    messages.append({
        "role": "user",
        "content": (
            f"My current code:\n{payload.userCode}\n\n"
            f"Task specifications:\n{payload.taskInstructions}\n\n"
            "Please evaluate my work Socratically."
        )
    })

    try:
        # Use the highly compliant, flagship 2026 reasoning model 'openai/gpt-oss-120b'
        response = groq_client.chat.completions.create(
            model="openai/gpt-oss-120b",
            messages=messages,
            temperature=0.3, # Keep temperature low for high rule-following accuracy
            max_completion_tokens=350
        )
        
        raw_completion = response.choices[0].message.content
        
        # Pass the output through our hard backend filter
        final_safe_response = enforce_socratic_guardrail(raw_completion)

        return {"guidance": final_safe_response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference failure: {str(e)}")

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
