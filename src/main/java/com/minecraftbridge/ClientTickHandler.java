package com.minecraftbridge;

import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;

/**
 * Client Tick Handler - Integration Layer
 *
 * This class serves as the central integration point for all AI subsystems.
 * It ensures that actions queued by BridgeServer are executed on the game thread,
 * and that UnpauseHandler keeps the game ticking even when paused.
 *
 * Architecture:
 * - Registered as a Forge event subscriber
 * - Processes BridgeServer action queue every tick
 * - UnpauseHandler processes its own ticking automatically
 * - All operations are thread-safe and non-blocking
 *
 * @version 3.0.0
 * @author Senior Engineering Team
 */
@Mod.EventBusSubscriber(modid = ExampleMod.MODID, value = Dist.CLIENT)
public class ClientTickHandler {

    /**
     * Main client tick event handler.
     * Called every game tick by Forge event system.
     *
     * Responsibilities:
     * 1. Process BridgeServer action queue (if server is running)
     * 2. UnpauseHandler handles its own ticking via @SubscribeEvent
     *
     * @param event The client tick event (contains phase information)
     */
    @SubscribeEvent
    public static void onClientTick(TickEvent.ClientTickEvent event) {
        // Only process on END phase to avoid double-processing
        if (event.phase != TickEvent.Phase.END) {
            return;
        }

        // Process BridgeServer action queue if server is running
        BridgeServer instance = BridgeServer.getInstance();
        if (instance != null && instance.isRunning()) {
            try {
                instance.processActionQueue();
            } catch (Exception e) {
                System.err.println("[ClientTickHandler] Error processing BridgeServer action queue: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Note: UnpauseHandler.onClientTick() is called automatically via its own @SubscribeEvent
        // Note: MinecraftAIManager runs on its own scheduled executor
    }
}
