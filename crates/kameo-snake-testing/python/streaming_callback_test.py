"""
Test for streaming callbacks - Python receives multiple items from a single callback
"""

import asyncio
import logging
import kameo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def handle_message_async(message):
    """
    Test handler that calls streaming callbacks and processes all stream items
    """
    try:
        logger.info(f"📨 Received message: {message}")
        
        if "CallbackRoundtrip" in message:
            value = message["CallbackRoundtrip"]["value"]
            logger.info(f"🔄 Starting streaming callback with value={value}")
            
            # Call kameo.callback_handle - this returns a Python async iterator
            logger.info(f"🌊 Starting streaming callback - getting async iterator")
            async_iterator = await kameo.callback_handle({'value': value})
            
            logger.info(f"📤 Got async iterator, starting async for loop")
            
            # Use async for to iterate through the stream items one by one
            stream_count = 0
            async for item in async_iterator:
                stream_count += 1
                logger.info(f"📦 Received stream item {stream_count} via async iteration: {item}")
            
            logger.info(f"✅ Async iteration completed, received {stream_count} total stream items")
            
            return {'CallbackRoundtripResult': {'value': value + 1}}
        else:
            raise ValueError(f"Unknown message type: {message}")
            
    except Exception as e:
        import traceback
        logger.error(f"❌ Exception: {e}")
        traceback.print_exc()
        raise