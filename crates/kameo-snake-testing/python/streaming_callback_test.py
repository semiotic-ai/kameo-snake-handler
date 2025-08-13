"""
Test for dynamic callbacks - Python calls different callback handlers by name
"""

import asyncio
import logging
import kameo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def handle_message_async(message):
    """
    Test handler that demonstrates calling different callback handlers by name
    """
    try:
        logger.info(f"📨 Received message: {message}")
        
        if "CallbackRoundtrip" in message:
            value = message["CallbackRoundtrip"]["value"]
            logger.info(f"🔄 Starting dynamic callback demonstration with value={value}")
            
            # Test all three dynamic callback handlers
            
            # 1. Test streaming callback handler
            logger.info(f"📞 Calling kameo.test.StreamingCallback()")
            streaming_data = {'value': value}
            streaming_iterator = await kameo.test.StreamingCallback(streaming_data)
            
            logger.info(f"🌊 Processing streaming callback items")
            stream_count = 0
            async for item in streaming_iterator:
                stream_count += 1
                logger.info(f"📦 Streaming item {stream_count}: {item}")
            
            # 2. Test basic callback handler  
            logger.info(f"📞 Calling kameo.basic.TestCallback()")
            basic_data = {'value': value + 10}
            basic_iterator = await kameo.basic.TestCallback(basic_data)
            
            logger.info(f"⚡ Processing basic callback items")
            basic_count = 0
            async for item in basic_iterator:
                basic_count += 1
                logger.info(f"📦 Basic item {basic_count}: {item}")
            
            # 3. Test trader callback handler
            logger.info(f"📞 Calling kameo.trader.TraderCallback()")
            trader_data = {'value': value + 20}
            trader_iterator = await kameo.trader.TraderCallback(trader_data)
            
            logger.info(f"💼 Processing trader callback items")
            trader_count = 0
            async for item in trader_iterator:
                trader_count += 1
                logger.info(f"📦 Trader item {trader_count}: {item}")
            
            logger.info(f"✅ Dynamic callback test completed! Processed {stream_count} streaming, {basic_count} basic, {trader_count} trader items")
            
            return {
                'CallbackRoundtripResult': {
                    'value': value + 1,
                    'streaming_items': stream_count,
                    'basic_items': basic_count,
                    'trader_items': trader_count
                }
            }
        else:
            raise ValueError(f"Unknown message type: {message}")
            
    except Exception as e:
        import traceback
        logger.error(f"❌ Exception: {e}")
        traceback.print_exc()
        raise