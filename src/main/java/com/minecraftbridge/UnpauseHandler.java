package com.minecraftbridge;

import net.minecraft.client.Minecraft;
import net.minecraft.client.gui.screens.PauseScreen;
import net.minecraft.client.gui.screens.Screen;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.server.MinecraftServer;
import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.event.TickEvent;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Robust UnpauseHandler - Keeps Minecraft world ticking even when:
 * - The pause screen is open
 * - Minecraft window loses focus
 * - Game would normally be paused
 *
 * This allows AI to continue controlling the player and processing the world
 * in real-time without interruption.
 */
@Mod.EventBusSubscriber(modid = ExampleMod.MODID, value = Dist.CLIENT)
public class UnpauseHandler {

    private static final Logger LOGGER = LogManager.getLogger("UnpauseHandler");
    private static final Minecraft mc = Minecraft.getInstance();
    private static boolean forceUnpaused = false;
    private static int ticksSinceLastForce = 0;
    private static volatile int forceTickInterval = 1; // Tick every frame when forced

    /**
     * Enable forced unpausing - call this when AI takes control
     */
    public static void enableForceUnpause() {
        forceUnpaused = true;
        LOGGER.info("Force unpause enabled - world will continue ticking");
    }

    /**
     * Disable forced unpausing - restore normal pause behavior
     */
    public static void disableForceUnpause() {
        forceUnpaused = false;
        LOGGER.info("Force unpause disabled - normal pause behavior restored");
    }

    /**
     * Check if forced unpausing is currently active
     */
    public static boolean isForceUnpauseEnabled() {
        return forceUnpaused;
    }

    /**
     * Main tick event handler - called every client tick
     */
    @SubscribeEvent
    public static void onClientTick(TickEvent.ClientTickEvent event) {
        // Only process on END phase to avoid double-ticking
        if (event.phase != TickEvent.Phase.END) {
            return;
        }

        ticksSinceLastForce++;

        // Force tick if enabled and conditions are met
        if (forceUnpaused && shouldForceTick()) {
            if (ticksSinceLastForce >= forceTickInterval) {
                forceTick();
                ticksSinceLastForce = 0;
            }
        }
    }

    /**
     * Determine if we should force tick the world
     */
    private static boolean shouldForceTick() {
        if (mc == null) return false;

        // Force tick if game is paused (window lost focus or pause screen open)
        if (mc.isPaused()) return true;

        // Force tick if pause screen is open even if not technically paused
        Screen currentScreen = mc.screen;
        if (currentScreen instanceof PauseScreen) return true;

        return false;
    }

    /**
     * Force the game to tick while paused
     * This is the core function that keeps everything running
     */
    public static void forceTick() {
        if (mc == null) {
            LOGGER.warn("Minecraft instance is null, cannot force tick");
            return;
        }

        try {
            // 1. Tick the client level (world)
            ClientLevel level = mc.level;
            if (level != null) {
                try {
                    // Tick world logic (block updates, random ticks, etc.)
                    level.tick(() -> true);

                    // Tick all entities in the world
                    level.tickEntities();

                    LOGGER.debug("World ticked successfully");
                } catch (Exception e) {
                    LOGGER.error("Error ticking world: {}", e.getMessage(), e);
                }
            } else {
                LOGGER.debug("Level is null, skipping world tick");
            }

            // 2. Tick the player
            if (mc.player != null) {
                try {
                    // Base tick handles basic player updates
                    mc.player.baseTick();

                    // Also tick player's AI to process movement/actions
                    mc.player.tick();

                    LOGGER.debug("Player ticked successfully");
                } catch (Exception e) {
                    LOGGER.error("Error ticking player: {}", e.getMessage(), e);
                }
            } else {
                LOGGER.debug("Player is null, skipping player tick");
            }

            // 3. Tick sound manager to keep audio playing
            if (mc.getSoundManager() != null) {
                try {
                    mc.getSoundManager().tick(false);
                } catch (Exception e) {
                    LOGGER.error("Error ticking sound manager: {}", e.getMessage(), e);
                }
            }

            // 4. Tick particle engine to keep particles animating
            if (mc.particleEngine != null) {
                try {
                    mc.particleEngine.tick();
                } catch (Exception e) {
                    LOGGER.error("Error ticking particle engine: {}", e.getMessage(), e);
                }
            }

            // 5. Tick integrated server (singleplayer only)
            if (mc.hasSingleplayerServer()) {
                MinecraftServer server = mc.getSingleplayerServer();
                if (server != null) {
                    try {
                        // Tick the server to process game logic, mob AI, world generation
                        server.tickServer(() -> true);

                        LOGGER.debug("Integrated server ticked successfully");
                    } catch (Exception e) {
                        LOGGER.error("Error ticking integrated server: {}", e.getMessage(), e);
                    }
                } else {
                    LOGGER.debug("Integrated server is null");
                }
            }

            // 6. Process any queued actions from BridgeServer
            BridgeServer instance = BridgeServer.getInstance();
            if (instance != null && instance.isRunning()) {
                try {
                    instance.processActionQueue();
                } catch (Exception e) {
                    LOGGER.error("Error processing action queue: {}", e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            LOGGER.error("Critical error in forceTick: {}", e.getMessage(), e);
        }
    }

    /**
     * Force a single tick immediately - useful for synchronous operations
     * Call this before executing AI commands to ensure world is updated
     */
    public static void forceTickNow() {
        if (!forceUnpaused) {
            LOGGER.debug("Force tick requested but force unpause is disabled");
        }
        forceTick();
    }

    /**
     * Reset the tick counter - useful for precise timing control
     */
    public static void resetTickCounter() {
        ticksSinceLastForce = 0;
    }

    /**
     * Set the interval between forced ticks (in client ticks)
     * Default is 1 (every tick). Higher values reduce CPU usage but may cause stuttering.
     */
    public static void setForceTickInterval(int interval) {
        if (interval < 1) {
            LOGGER.warn("Force tick interval must be at least 1, got {}", interval);
            return;
        }
        forceTickInterval = interval;
        LOGGER.info("Force tick interval set to {}", interval);
    }

    /**
     * Get current pause state information - useful for debugging
     */
    public static String getPauseStateInfo() {
        if (mc == null) return "Minecraft instance null";

        StringBuilder info = new StringBuilder();
        info.append("ForceUnpause: ").append(forceUnpaused);
        info.append(", IsPaused: ").append(mc.isPaused());
        info.append(", Screen: ").append(mc.screen != null ? mc.screen.getClass().getSimpleName() : "null");
        info.append(", Level: ").append(mc.level != null ? "present" : "null");
        info.append(", Player: ").append(mc.player != null ? "present" : "null");

        return info.toString();
    }
}
