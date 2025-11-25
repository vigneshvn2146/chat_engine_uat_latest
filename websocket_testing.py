import asyncio
import websockets
import json

# Replace with your target WebSocket endpoint
WS_URL = "wss://64eyodck3b.execute-api.ap-south-1.amazonaws.com/uat/"

async def connect_to_websocket():
    async with websockets.connect(WS_URL) as websocket:
        print(f"Connected to {WS_URL}")

        # Example payload — modify for your API
        request_payload = {'TopicID': 'c9c9dacb-4fee-4eae-ab63-fbcd0ceef8bc', 'Prompt': 'How to do MAN B&W 6S50MC engine maintenance and operation', 'IsNewTopic': 'True'}

        # Send message
        await websocket.send(json.dumps(request_payload))
        print("✅ Message sent:", request_payload)

        # Continuously listen for messages
        try:
            async for message in websocket:
                print("📩 Received message:", repr(json.loads(message).get("Response",{}).get("response",{})))

        except websockets.ConnectionClosed:
            print("❌ Connection closed")

if __name__ == "__main__":
    asyncio.run(connect_to_websocket())
