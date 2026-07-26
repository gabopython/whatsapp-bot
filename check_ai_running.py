import asyncio
from agent_client import AgentClient  # Imports the class from agent_client.py

async def run_test():
    client = AgentClient()
    prompt = "how to dance on1"
    
    try:
        print(f"Sending prompt to {client.MODEL_AGENT}...")
        
        response = await client.generate_agent(prompt)
        
        print("\n--- AI Response ---")
        print(response)
        print("-------------------\n")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(run_test())