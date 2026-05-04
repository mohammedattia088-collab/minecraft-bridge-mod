package com.minecraftbridge;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.minecraft.advancements.AdvancementHolder;
import net.minecraft.advancements.AdvancementProgress;
import net.minecraft.SharedConstants;
import net.minecraft.client.Minecraft;
import net.minecraft.client.multiplayer.ClientLevel;
import net.minecraft.client.player.LocalPlayer;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.core.Holder;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.network.chat.Component;
import net.minecraft.network.protocol.game.ServerboundMovePlayerPacket;
import net.minecraft.resources.ResourceKey;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.server.MinecraftServer;
import net.minecraft.server.PlayerAdvancements;
import net.minecraft.server.ServerAdvancementManager;
import net.minecraft.server.level.ServerPlayer;
import net.minecraft.tags.BlockTags;
import net.minecraft.world.Difficulty;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.effect.MobEffect;
import net.minecraft.world.effect.MobEffectInstance;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.EquipmentSlot;
import net.minecraft.world.entity.LivingEntity;
import net.minecraft.world.entity.MoverType;
import net.minecraft.world.entity.animal.Animal;
import net.minecraft.world.entity.item.ItemEntity;
import net.minecraft.world.entity.monster.Monster;
import net.minecraft.world.entity.npc.Villager;
import net.minecraft.world.entity.player.Inventory;
import net.minecraft.world.entity.projectile.Projectile;
import net.minecraft.world.food.FoodProperties;
import net.minecraft.world.inventory.AbstractContainerMenu;
import net.minecraft.world.inventory.ClickType;
import net.minecraft.world.inventory.CraftingMenu;
import net.minecraft.world.inventory.InventoryMenu;
import net.minecraft.world.inventory.RecipeBookMenu;
import net.minecraft.world.inventory.Slot;
import net.minecraft.world.item.Item;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.item.crafting.CraftingRecipe;
import net.minecraft.world.item.crafting.Ingredient;
import net.minecraft.world.item.crafting.RecipeHolder;
import net.minecraft.world.item.crafting.RecipeType;
import net.minecraft.world.item.crafting.ShapedRecipe;
import net.minecraft.world.item.crafting.ShapelessRecipe;
import net.minecraft.world.item.enchantment.Enchantment;
import net.minecraft.world.item.enchantment.EnchantmentHelper;
import net.minecraft.world.item.enchantment.ItemEnchantments;
import net.minecraft.world.level.ClipContext;
import net.minecraft.world.level.GameType;
import net.minecraft.world.level.LightLayer;
import net.minecraft.world.level.biome.Biome;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.phys.AABB;
import net.minecraft.world.phys.BlockHitResult;
import net.minecraft.world.phys.EntityHitResult;
import net.minecraft.world.phys.HitResult;
import net.minecraft.world.phys.Vec3;
import net.minecraftforge.fml.ModList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * BridgeServer - Production-Grade Minecraft RL Bridge
 *
 * <p>Provides a thread-safe, high-performance JSON-over-TCP API for external agents
 * to perceive and control Minecraft client state. Optimized for reinforcement learning,
 * automated testing, AI research, and multi-agent coordination.
 *
 * <h2>Architecture</h2>
 * <ul>
 *   <li><b>Accept Thread:</b> Handles incoming TCP connections on localhost</li>
 *   <li><b>Client Pool (8 threads):</b> Processes commands from connected clients</li>
 *   <li><b>Broadcaster (1 thread):</b> Generates and sends periodic state snapshots</li>
 *   <li><b>Game Thread:</b> All Minecraft state modifications via action queue</li>
 * </ul>
 *
 * <h2>Threading Model</h2>
 * <p>Commands are received on client handler threads, parsed, and queued as Runnables.
 * The mod's client tick handler must call {@link #processActionQueue()} to execute
 * queued actions on the game thread. This ensures thread safety and prevents
 * ConcurrentModificationExceptions.
 *
 * <h2>Integration Example</h2>
 * <pre>{@code
 * // In mod initialization (FMLClientSetupEvent or similar)
 * BridgeServer.start(25575, "optional_auth_token");
 *
 * // In client tick event (ClientTickEvent.Post)
 * @SubscribeEvent
 * public void onClientTick(ClientTickEvent.Post event) {
 *     BridgeServer server = BridgeServer.getInstance();
 *     if (server != null && server.isRunning()) {
 *         server.processActionQueue();
 *     }
 * }
 *
 * // On shutdown
 * BridgeServer.stop();
 * }</pre>
 *
 * <h2>Command Protocol</h2>
 * <p>Commands are JSON objects sent over TCP, one per line:
 * <pre>{"action": "move", "forward": 1.0, "strafe": 0.0}</pre>
 * <p>Responses are JSON objects:
 * <pre>{"status": "success", "action_id": 12345, "timestamp": 1234567890}</pre>
 *
 * <h2>State Snapshots</h2>
 * <p>Periodic snapshots are broadcast to all connected clients containing:
 * <ul>
 *   <li>Player vitals (health, food, XP)</li>
 *   <li>Position, orientation, motion</li>
 *   <li>Inventory state with enchantments</li>
 *   <li>Environment (biome, weather, time, light)</li>
 *   <li>Nearby entities and blocks</li>
 *   <li>Server metrics</li>
 * </ul>
 *
 * <h2>Commands Implemented</h2>
 * <p>This server implements 75+ commands across categories:
 * <ul>
 *   <li><b>Movement:</b> move, look, jump, sprint, sneak, swim, stop_moving</li>
 *   <li><b>Combat:</b> attack, use, block, target_entity, flee</li>
 *   <li><b>Inventory:</b> select_slot, swap_slots, drop_item, equip_armor, craft</li>
 *   <li><b>World:</b> mine, place, get_block_info, scan_blocks, find_nearest_block</li>
 *   <li><b>Entity:</b> interact_entity, ride_entity, trade_with_villager</li>
 *   <li><b>Utility:</b> ping, get_metrics, get_full_state, stop_server</li>
 * </ul>
 *
 * @author Minecraft RL Team
 * @version 4.0.1
 * @since Forge
 */
public class BridgeServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(BridgeServer.class);

    private static final String BRIDGE_VERSION = "4.0.1";
    private static final int BRIDGE_API_LEVEL = 4;
    private static final int DEFAULT_PORT = 25575;
    private static final int MAX_ACTIONS_PER_TICK = 50;
    private static final int ACTION_HISTORY_SIZE = 10000;
    private static final int BACKLOG_WARNING_THRESHOLD = 100;
    private static final int BACKLOG_CRITICAL_THRESHOLD = 500;
    private static final double MAX_REACH_DISTANCE = 5.0;
    private static final int MAX_NEARBY_ENTITIES = 100;
    private static final int MAX_NEARBY_BLOCKS = 1000;
    private static final int MAX_VISIBLE_ENTITIES = 32;
    private static final int MAX_THREATS = 24;
    private static final int MAX_HAZARDS = 64;
    private static final int DEFAULT_VISION_RAYS = 9;
    private static final int MAX_VISION_RAYS = 31;
    private static final double MAX_RAYCAST_DISTANCE = 96.0;
    private static final double COMBAT_REACH_DISTANCE = 4.5;
    private static final double DEFAULT_COMBAT_RADIUS = 8.0;
    private static final double DEFAULT_THREAT_RADIUS = 20.0;
    private static final int MAX_SCAN_RADIUS = 64;
    private static final String[] VANILLA_ADVANCEMENT_IDS = {
            "story/root", "story/mine_stone", "story/upgrade_tools", "story/smelt_iron",
            "story/obtain_armor", "story/lava_bucket", "story/iron_tools", "story/deflect_arrow",
            "story/form_obsidian", "story/mine_diamond", "story/enter_the_nether", "story/shiny_gear",
            "story/enchant_item", "story/cure_zombie_villager", "story/follow_ender_eye", "story/enter_the_end",
            "nether/root", "nether/return_to_sender", "nether/find_bastion", "nether/obtain_ancient_debris",
            "nether/fast_travel", "nether/find_fortress", "nether/obtain_crying_obsidian",
            "nether/distract_piglin", "nether/ride_strider", "nether/uneasy_alliance", "nether/loot_bastion",
            "nether/use_lodestone", "nether/netherite_armor", "nether/get_wither_skull",
            "nether/obtain_blaze_rod", "nether/charge_respawn_anchor",
            "nether/ride_strider_in_overworld_lava", "nether/explore_nether", "nether/summon_wither",
            "nether/brew_potion", "nether/create_beacon", "nether/all_potions",
            "nether/create_full_beacon", "nether/all_effects",
            "end/root", "end/kill_dragon", "end/dragon_egg", "end/enter_end_gateway",
            "end/respawn_dragon", "end/dragon_breath", "end/find_end_city", "end/elytra", "end/levitate",
            "adventure/root", "adventure/voluntary_exile", "adventure/spyglass_at_parrot",
            "adventure/kill_a_mob", "adventure/trade", "adventure/trim_with_any_armor_pattern",
            "adventure/honey_block_slide", "adventure/ol_betsy",
            "adventure/lightning_rod_with_villager_no_fire", "adventure/fall_from_world_height",
            "adventure/avoid_vibration", "adventure/sleep_in_bed", "adventure/hero_of_the_village",
            "adventure/spyglass_at_ghast", "adventure/throw_trident",
            "adventure/kill_mob_near_sculk_catalyst", "adventure/shoot_arrow",
            "adventure/kill_all_mobs", "adventure/totem_of_undying",
            "adventure/summon_iron_golem", "adventure/trade_at_world_height",
            "adventure/two_birds_one_arrow", "adventure/whos_the_pillager_now",
            "adventure/arbalistic", "adventure/adventuring_time",
            "adventure/play_jukebox_in_meadows", "adventure/walk_on_powder_snow_with_leather_boots",
            "adventure/light_bucket", "adventure/remove_oxidation_wax_on_copper",
            "adventure/salvage_sherd", "adventure/trim_with_all_exclusive_armor_patterns",
            "adventure/craft_decorated_pot_using_only_sherds", "adventure/read_power_of_chiseled_bookshelf",
            "adventure/minecraft_trials_edition", "adventure/crafters_crafting_crafters",
            "adventure/blowback", "adventure/overoverkill", "adventure/lighten_up",
            "adventure/under_lock_and_key", "adventure/revaulting", "adventure/who_needs_rockets",
            "adventure/brush_armadillo", "adventure/spyglass_at_dragon", "adventure/sniper_duel",
            "adventure/bullseye", "adventure/very_very_frightening",
            "husbandry/root", "husbandry/safely_harvest_honey", "husbandry/breed_an_animal",
            "husbandry/allay_deliver_item_to_player", "husbandry/ride_a_boat_with_a_goat",
            "husbandry/tame_an_animal", "husbandry/make_a_sign_glow", "husbandry/fishy_business",
            "husbandry/silk_touch_nest", "husbandry/tadpole_in_a_bucket", "husbandry/plant_seed",
            "husbandry/wax_on", "husbandry/bred_all_animals", "husbandry/allay_deliver_cake_to_note_block",
            "husbandry/complete_catalogue", "husbandry/tactical_fishing",
            "husbandry/leash_all_frog_variants", "husbandry/balanced_diet",
            "husbandry/obtain_sniffer_egg", "husbandry/froglights",
            "husbandry/plant_any_sniffer_seed", "husbandry/wax_off",
            "husbandry/axolotl_in_a_bucket", "husbandry/kill_axolotl_target",
            "husbandry/obtain_netherite_hoe", "husbandry/feed_snifflet",
            "husbandry/remove_wolf_armor", "husbandry/repair_wolf_armor", "husbandry/whole_pack"
    };

    private static volatile BridgeServer INSTANCE;

    private final int port;
    private final String authToken;
    private final BridgeConfig config;

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private final ExecutorService clientPool;
    private final ScheduledExecutorService broadcaster;
    private final Gson gson;
    private final AtomicBoolean running;

    private final Set<ClientHandler> activeClients;
    private final ConcurrentLinkedQueue<Runnable> actionQueue;
    private final ConcurrentHashMap<Long, ActionRecord> actionHistory;
    private final ConcurrentHashMap<String, BlockPos> waypoints;
    private final ConcurrentHashMap<BlockPos, MiningState> activeMining;
    private final ControlState controlState;
    private NavigationState navigationState;

    private final AtomicLong commandsProcessed;
    private final AtomicLong actionsExecuted;
    private final AtomicLong actionIdCounter;
    private final AtomicLong snapshotIdCounter;
    private final long serverStartTime;

    /**
     * Configuration object for BridgeServer tuning parameters.
     */
    public static class BridgeConfig {
        public int maxActionsPerTick = 50;
        public int broadcastIntervalMs = 1000;
        public int nearbyEntityRadius = 32;
        public int nearbyBlockRadius = 16;
        public int blockSampleDensity = 2;
        public double maxReachDistance = 5.25;
        public double maxRaycastDistance = 96.0;
        public double combatReachDistance = 4.5;
        public double threatAssessmentRadius = 20.0;
        public double autoAttackRadius = 8.0;
        public boolean protectedZonesEnabled = false;
        public boolean requireAuth = false;
        public int commandRateLimit = 100;
        public int commandBurst = 200;
        public boolean enableDeltaSnapshots = false;

        public BridgeConfig() {}
    }

    /**
     * Private constructor for singleton pattern.
     *
     * @param port TCP port to bind to
     * @param authToken Optional authentication token (null to disable)
     * @param config Configuration parameters
     */
    private BridgeServer(int port, String authToken, BridgeConfig config) {
        this.port = port;
        this.authToken = authToken;
        this.config = config != null ? config : new BridgeConfig();
        this.serverStartTime = System.currentTimeMillis();

        this.clientPool = Executors.newFixedThreadPool(8, r -> {
            Thread t = new Thread(r, "BridgeServer-Client");
            t.setDaemon(true);
            return t;
        });

        this.broadcaster = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "BridgeServer-Broadcaster");
            t.setDaemon(true);
            return t;
        });

        this.gson = new Gson();
        this.running = new AtomicBoolean(false);

        this.activeClients = ConcurrentHashMap.newKeySet();
        this.actionQueue = new ConcurrentLinkedQueue<>();
        this.actionHistory = new ConcurrentHashMap<>();
        this.waypoints = new ConcurrentHashMap<>();
        this.activeMining = new ConcurrentHashMap<>();
        this.controlState = new ControlState();

        this.commandsProcessed = new AtomicLong(0);
        this.actionsExecuted = new AtomicLong(0);
        this.actionIdCounter = new AtomicLong(0);
        this.snapshotIdCounter = new AtomicLong(0);
    }

    /**
     * Start the BridgeServer singleton with default configuration.
     *
     * @param port TCP port to listen on
     * @param authToken Optional authentication token (null to disable auth)
     * @return The BridgeServer instance
     */
    public static synchronized BridgeServer start(int port, String authToken) {
        return start(port, authToken, new BridgeConfig());
    }

    /**
     * Start the BridgeServer singleton with custom configuration.
     *
     * @param port TCP port to listen on
     * @param authToken Optional authentication token (null to disable auth)
     * @param config Custom configuration parameters
     * @return The BridgeServer instance
     */
    public static synchronized BridgeServer start(int port, String authToken, BridgeConfig config) {
        if (INSTANCE == null) {
            INSTANCE = new BridgeServer(port, authToken, config);
            INSTANCE.startServer();
        } else if (!INSTANCE.running.get()) {
            INSTANCE.startServer();
        } else {
            LOGGER.warn("BridgeServer already running on port {}", INSTANCE.port);
        }
        return INSTANCE;
    }

    /**
     * Start the BridgeServer with default port and no authentication.
     *
     * @return The BridgeServer instance
     */
    public static BridgeServer startDefault() {
        return start(DEFAULT_PORT, null);
    }

    /**
     * Get the singleton BridgeServer instance.
     *
     * @return The BridgeServer instance, or null if not started
     */
    public static BridgeServer getInstance() {
        return INSTANCE;
    }

    /**
     * Stop the BridgeServer and clean up all resources.
     */
    public static synchronized void stop() {
        if (INSTANCE != null) {
            INSTANCE.stopServer();
            INSTANCE = null;
        }
    }

    /**
     * Check if the BridgeServer is currently running.
     *
     * @return true if server is running and accepting connections
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Process queued actions on the main game thread.
     * <p>
     * MUST be called from the client tick event handler. This method executes
     * up to MAX_ACTIONS_PER_TICK queued actions per tick to prevent game lag.
     * Actions are commands received from connected clients that modify game state.
     * <p>
     * Thread Safety: This method must only be called from the main game thread.
     */
    public void processActionQueue() {
        int processed = 0;
        Runnable action;

        while ((action = actionQueue.poll()) != null && processed < config.maxActionsPerTick) {
            try {
                action.run();
                actionsExecuted.incrementAndGet();
                processed++;
            } catch (Throwable e) {
                rethrowIfCritical(e);
                LOGGER.error("Error executing queued action: {}", e.getMessage(), e);
            }
        }

        int remaining = actionQueue.size();
        if (remaining > BACKLOG_WARNING_THRESHOLD) {
            LOGGER.warn("Action queue backlog: {} actions pending", remaining);
        }
        if (remaining > BACKLOG_CRITICAL_THRESHOLD) {
            LOGGER.error("Action queue critical: {} actions pending - consider increasing tick rate", remaining);
        }

        Minecraft mc = Minecraft.getInstance();
        if (mc.player != null && mc.level != null) {
            updateNavigation(mc);
            updateMiningProgress(mc);
            applyControlState(mc);
        }
    }

    /**
     * Get server metrics for monitoring and debugging.
     *
     * @return Map of metric names to values
     */
    public Map<String, Long> getMetrics() {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put("commands_processed", commandsProcessed.get());
        metrics.put("actions_executed", actionsExecuted.get());
        metrics.put("queue_size", (long) actionQueue.size());
        metrics.put("active_clients", (long) activeClients.size());
        metrics.put("action_history_size", (long) actionHistory.size());
        metrics.put("uptime_ms", System.currentTimeMillis() - serverStartTime);
        metrics.put("snapshots_sent", snapshotIdCounter.get());
        return metrics;
    }

    private void startServer() {
        if (running.get()) {
            LOGGER.info("BridgeServer already running on port {}", port);
            return;
        }

        try {
            serverSocket = new ServerSocket(port, 50, InetAddress.getByName("127.0.0.1"));
            running.set(true);

            LOGGER.info("╔════════════════════════════════════════════════╗");
            LOGGER.info("║  BridgeServer v{} Started Successfully     ║", BRIDGE_VERSION);
            LOGGER.info("║  Listening on: 127.0.0.1:{}                 ║", port);
            LOGGER.info("║  Authentication: {}                          ║",
                    authToken != null ? "ENABLED" : "DISABLED");
            LOGGER.info("║  Commands: 75+                                 ║");
            LOGGER.info("║  Thread Pool: 8 workers                        ║");
            LOGGER.info("╚════════════════════════════════════════════════╝");

        } catch (IOException e) {
            running.set(false);
            LOGGER.error("Failed to bind to 127.0.0.1:{} - {}", port, e.getMessage());
            return;
        }

        acceptThread = new Thread(this::acceptLoop, "BridgeServer-Accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        broadcaster.scheduleAtFixedRate(
                this::broadcastGameState,
                config.broadcastIntervalMs,
                config.broadcastIntervalMs,
                TimeUnit.MILLISECONDS
        );
        broadcaster.scheduleAtFixedRate(
                this::pumpActionQueueOnGameThread,
                50L,
                50L,
                TimeUnit.MILLISECONDS
        );
    }

    private void pumpActionQueueOnGameThread() {
        if (!running.get()) {
            return;
        }
        Minecraft mc = Minecraft.getInstance();
        mc.execute(() -> {
            try {
                processActionQueue();
            } catch (Throwable e) {
                rethrowIfCritical(e);
                LOGGER.error("Error pumping action queue: {}", e.getMessage(), e);
            }
        });
    }

    private void acceptLoop() {
        while (running.get() && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                client.setSoTimeout(30000);
                client.setTcpNoDelay(true);

                LOGGER.info("Client connected: {}", client.getRemoteSocketAddress());

                ClientHandler handler = new ClientHandler(client);
                activeClients.add(handler);
                clientPool.submit(handler);

            } catch (SocketException se) {
                if (running.get()) {
                    LOGGER.error("Socket exception in accept loop: {}", se.getMessage());
                }
            } catch (IOException ioe) {
                if (running.get()) {
                    LOGGER.error("I/O exception accepting client: {}", ioe.getMessage());
                }
            }
        }
        LOGGER.info("Accept thread exiting");
    }

    private void stopServer() {
        if (!running.getAndSet(false)) {
            return;
        }

        LOGGER.info("Shutting down BridgeServer...");

        broadcaster.shutdown();

        for (ClientHandler client : activeClients) {
            client.disconnect();
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            LOGGER.warn("Error closing server socket: {}", e.getMessage());
        }

        if (acceptThread != null) {
            acceptThread.interrupt();
            try {
                acceptThread.join(2000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        clientPool.shutdownNow();

        try {
            if (!clientPool.awaitTermination(3, TimeUnit.SECONDS)) {
                LOGGER.warn("Client pool did not terminate cleanly");
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        activeClients.clear();
        actionQueue.clear();
        actionHistory.clear();
        waypoints.clear();
        activeMining.clear();

        LOGGER.info("╔════════════════════════════════════════════════╗");
        LOGGER.info("║  BridgeServer Stopped Successfully            ║");
        LOGGER.info("║  Commands Processed: {}                    ║", commandsProcessed.get());
        LOGGER.info("║  Actions Executed: {}                      ║", actionsExecuted.get());
        LOGGER.info("║  Uptime: {} seconds                        ║",
                (System.currentTimeMillis() - serverStartTime) / 1000);
        LOGGER.info("╚════════════════════════════════════════════════╝");
    }

    private void broadcastGameState() {
        if (activeClients.isEmpty()) {
            return;
        }

        try {
            Minecraft mc = Minecraft.getInstance();
            if (mc.player == null || mc.level == null) {
                return;
            }

            JsonObject state = new JsonObject();
            state.addProperty("type", "game_state");
            state.addProperty("snapshot_id", snapshotIdCounter.incrementAndGet());
            state.addProperty("timestamp", System.currentTimeMillis());
            state.addProperty("tick", mc.level.getGameTime());

            state.add("player", buildPlayerState(mc));
            state.add("inventory", buildInventoryState(mc));
            state.add("environment", buildEnvironmentState(mc));
            state.add("nearby_entities", buildNearbyEntities(mc));
            state.add("nearby_blocks", buildNearbyBlocks(mc));
            state.add("vision", buildVisionState(mc));
            state.add("threats", buildThreatAssessment(mc));
            state.add("survival", buildSurvivalState(mc));
            state.add("combat", buildCombatState(mc));

            Map<String, Long> metrics = getMetrics();
            JsonObject metricsObj = new JsonObject();
            metrics.forEach(metricsObj::addProperty);
            state.add("metrics", metricsObj);

            String message = gson.toJson(state) + "\n";

            for (ClientHandler client : activeClients) {
                client.sendMessage(message);
            }

        } catch (Exception e) {
            LOGGER.debug("Error broadcasting game state: {}", e.getMessage());
        }
    }

    private JsonObject buildPlayerState(Minecraft mc) {
        JsonObject player = new JsonObject();
        LocalPlayer p = mc.player;

        player.addProperty("health", p.getHealth());
        player.addProperty("max_health", p.getMaxHealth());
        player.addProperty("is_alive", p.isAlive());
        player.addProperty("is_dead", p.isDeadOrDying());
        player.addProperty("food", p.getFoodData().getFoodLevel());
        player.addProperty("saturation", p.getFoodData().getSaturationLevel());
        player.addProperty("exhaustion", p.getFoodData().getExhaustionLevel());
        player.addProperty("xp_level", p.experienceLevel);
        player.addProperty("xp_progress", p.experienceProgress);
        player.addProperty("xp_total", p.totalExperience);

        Vec3 pos = p.position();
        JsonObject position = new JsonObject();
        position.addProperty("x", pos.x);
        position.addProperty("y", pos.y);
        position.addProperty("z", pos.z);
        player.add("position", position);

        player.addProperty("yaw", p.getYRot());
        player.addProperty("pitch", p.getXRot());

        Vec3 motion = p.getDeltaMovement();
        JsonObject motionObj = new JsonObject();
        motionObj.addProperty("x", motion.x);
        motionObj.addProperty("y", motion.y);
        motionObj.addProperty("z", motion.z);
        player.add("motion", motionObj);

        ItemStack mainHand = p.getMainHandItem();
        player.addProperty("main_hand", mainHand.isEmpty() ? "empty" :
                BuiltInRegistries.ITEM.getKey(mainHand.getItem()).toString());
        player.addProperty("main_hand_count", mainHand.getCount());

        ItemStack offHand = p.getOffhandItem();
        player.addProperty("off_hand", offHand.isEmpty() ? "empty" :
                BuiltInRegistries.ITEM.getKey(offHand.getItem()).toString());
        player.addProperty("off_hand_count", offHand.getCount());

        JsonObject armor = new JsonObject();
        for (EquipmentSlot slot : new EquipmentSlot[]{
                EquipmentSlot.HEAD, EquipmentSlot.CHEST,
                EquipmentSlot.LEGS, EquipmentSlot.FEET}) {
            ItemStack armorPiece = p.getItemBySlot(slot);
            armor.addProperty(slot.getName(), armorPiece.isEmpty() ? "empty" :
                    BuiltInRegistries.ITEM.getKey(armorPiece.getItem()).toString());
        }
        player.add("armor", armor);

        player.addProperty("armor_value", p.getArmorValue());
        player.addProperty("is_on_ground", p.onGround());
        player.addProperty("is_in_water", p.isInWater());
        player.addProperty("is_in_lava", p.isInLava());
        player.addProperty("is_sneaking", p.isShiftKeyDown());
        player.addProperty("is_sprinting", p.isSprinting());
        player.addProperty("is_swimming", p.isSwimming());
        player.addProperty("is_underwater", p.isUnderWater());
        player.addProperty("is_on_fire", p.isOnFire());
        player.addProperty("is_flying", p.getAbilities().flying);
        player.addProperty("can_fly", p.getAbilities().mayfly);
        player.addProperty("instant_build", p.getAbilities().instabuild);
        if (mc.gameMode != null && mc.gameMode.getPlayerMode() != null) {
            GameType mode = mc.gameMode.getPlayerMode();
            player.addProperty("game_mode", mode.getName());
            player.addProperty("is_creative_mode", mode.isCreative());
            player.addProperty("is_survival_mode", mode.isSurvival());
        }
        player.addProperty("is_riding", p.isPassenger());
        player.addProperty("is_sleeping", p.isSleeping());
        player.addProperty("fall_distance", p.fallDistance);
        player.addProperty("air", p.getAirSupply());
        player.addProperty("max_air", p.getMaxAirSupply());
        player.addProperty("selected_slot", p.getInventory().selected);

        JsonArray effects = new JsonArray();
        for (MobEffectInstance effectInstance : p.getActiveEffects()) {
            JsonObject effectObj = new JsonObject();
            Holder<MobEffect> effectHolder = effectInstance.getEffect();
            String effectId = effectHolder.unwrapKey()
                    .map(ResourceKey::location)
                    .map(Object::toString)
                    .orElse("unknown:effect");
            effectObj.addProperty("effect", effectId);
            effectObj.addProperty("duration", effectInstance.getDuration());
            effectObj.addProperty("amplifier", effectInstance.getAmplifier());
            effectObj.addProperty("ambient", effectInstance.isAmbient());
            effectObj.addProperty("visible", effectInstance.isVisible());
            effects.add(effectObj);
        }
        player.add("effects", effects);

        return player;
    }

    private JsonObject buildInventoryState(Minecraft mc) {
        JsonObject inventory = new JsonObject();
        JsonArray items = new JsonArray();

        Inventory inv = mc.player.getInventory();
        for (int i = 0; i < inv.getContainerSize(); i++) {
            ItemStack stack = inv.getItem(i);
            if (!stack.isEmpty()) {
                JsonObject item = new JsonObject();
                item.addProperty("slot", i);
                item.addProperty("item", BuiltInRegistries.ITEM.getKey(stack.getItem()).toString());
                item.addProperty("count", stack.getCount());
                item.addProperty("display_name", stack.getHoverName().getString());
                item.addProperty("max_stack_size", stack.getMaxStackSize());

                if (stack.isDamageableItem()) {
                    item.addProperty("damage", stack.getDamageValue());
                    item.addProperty("max_damage", stack.getMaxDamage());
                    item.addProperty("durability_remaining", stack.getMaxDamage() - stack.getDamageValue());
                    item.addProperty("durability_percent",
                            (1.0 - (double) stack.getDamageValue() / stack.getMaxDamage()) * 100.0);
                }

                ItemEnchantments enchantments = EnchantmentHelper.getEnchantmentsForCrafting(stack);
                if (!enchantments.isEmpty()) {
                    JsonArray enchArray = new JsonArray();
                    for (Map.Entry<Holder<Enchantment>, Integer> entry : enchantments.entrySet()) {
                        JsonObject enchObj = new JsonObject();
                        Holder<Enchantment> enchHolder = entry.getKey();
                        String enchId = enchHolder.unwrapKey()
                                .map(ResourceKey::location)
                                .map(Object::toString)
                                .orElse("unknown:enchantment");
                        enchObj.addProperty("enchantment", enchId);
                        enchObj.addProperty("level", entry.getValue());
                        enchArray.add(enchObj);
                    }
                    item.add("enchantments", enchArray);
                }

                FoodProperties food = resolveFoodProperties(stack, mc.player);
                item.addProperty("is_edible", food != null);
                if (food != null) {
                    item.addProperty("nutrition", food.nutrition());
                    item.addProperty("saturation", food.saturation());
                }
                item.addProperty("is_stackable", stack.isStackable());

                items.add(item);
            }
        }

        inventory.add("items", items);
        inventory.addProperty("total_items", items.size());

        JsonArray hotbar = new JsonArray();
        for (int i = 0; i < 9; i++) {
            ItemStack stack = inv.getItem(i);
            hotbar.add(stack.isEmpty() ? "empty" :
                    BuiltInRegistries.ITEM.getKey(stack.getItem()).toString());
        }
        inventory.add("hotbar", hotbar);

        return inventory;
    }

    private FoodProperties resolveFoodProperties(ItemStack stack, LocalPlayer player) {
        try {
            Method stackMethod = ItemStack.class.getMethod("getFoodProperties", LivingEntity.class);
            Object food = stackMethod.invoke(stack, player);
            if (food instanceof FoodProperties properties) {
                return properties;
            }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // Try the current data-component API below.
        }

        try {
            Class<?> componentTypeClass = Class.forName("net.minecraft.core.component.DataComponentType");
            Class<?> componentsClass = Class.forName("net.minecraft.core.component.DataComponents");
            Object foodComponentType = componentsClass.getField("FOOD").get(null);
            Method getComponent = ItemStack.class.getMethod("get", componentTypeClass);
            Object food = getComponent.invoke(stack, foodComponentType);
            if (food instanceof FoodProperties properties) {
                return properties;
            }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            // Older/newer mappings may expose this through Item instead.
        }

        try {
            Method itemMethod = stack.getItem().getClass()
                    .getMethod("getFoodProperties", ItemStack.class, LivingEntity.class);
            Object food = itemMethod.invoke(stack.getItem(), stack, player);
            if (food instanceof FoodProperties properties) {
                return properties;
            }
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }

        return null;
    }

    private JsonObject buildEnvironmentState(Minecraft mc) {
        JsonObject env = new JsonObject();
        ClientLevel level = mc.level;
        BlockPos playerPos = mc.player.blockPosition();
        MinecraftServer server = mc.getSingleplayerServer();

        if (server != null) {
            env.addProperty("world_name", server.getWorldData().getLevelName());
        }
        env.addProperty("dimension", level.dimension().location().toString());
        env.addProperty("biome", level.getBiome(playerPos).unwrapKey()
                .map(key -> key.location().toString())
                .orElse("unknown"));
        env.addProperty("is_raining", level.isRaining());
        env.addProperty("is_thundering", level.isThundering());
        env.addProperty("rain_level", level.getRainLevel(1.0f));
        env.addProperty("thunder_level", level.getThunderLevel(1.0f));
        env.addProperty("day_time", level.getDayTime());
        env.addProperty("game_time", level.getGameTime());
        env.addProperty("is_day", level.isDay());
        env.addProperty("is_night", level.isNight());
        env.addProperty("block_light", level.getBrightness(LightLayer.BLOCK, playerPos));
        env.addProperty("sky_light", level.getBrightness(LightLayer.SKY, playerPos));
        env.addProperty("combined_light", level.getMaxLocalRawBrightness(playerPos));
        env.addProperty("can_see_sky", level.canSeeSky(playerPos));
        env.addProperty("difficulty", level.getDifficulty().toString());
        env.addProperty("moon_phase", level.getMoonPhase());

        return env;
    }

    private JsonObject buildVisionState(Minecraft mc) {
        return buildVisionState(mc, null);
    }

    private JsonObject buildVisionState(Minecraft mc, JsonObject cmd) {
        JsonObject vision = new JsonObject();

        float yaw = getFloat(cmd, "yaw", mc.player.getYRot());
        float pitch = clampFloat(getFloat(cmd, "pitch", mc.player.getXRot()), -90.0f, 90.0f);
        double distance = clampDouble(getDouble(cmd, "distance", Math.min(config.maxRaycastDistance, 64.0)),
                0.5, config.maxRaycastDistance);
        int requestedRays = getInt(cmd, "rays", DEFAULT_VISION_RAYS);
        int rays = Math.max(1, Math.min(MAX_VISION_RAYS, requestedRays));
        if (rays % 2 == 0) {
            rays++;
        }
        double fov = clampDouble(getDouble(cmd, "fov", 70.0), 5.0, 120.0);

        vision.add("crosshair", describeRaycast(mc, yaw, pitch, Math.min(distance, config.maxReachDistance)));

        JsonArray scanRays = new JsonArray();
        if (rays == 1) {
            scanRays.add(describeRaycast(mc, yaw, pitch, distance));
        } else {
            for (int i = 0; i < rays; i++) {
                double t = (double) i / (double) (rays - 1);
                float rayYaw = (float) (yaw - fov / 2.0 + (fov * t));
                scanRays.add(describeRaycast(mc, rayYaw, pitch, distance));
            }
        }
        vision.add("scan_rays", scanRays);
        vision.addProperty("scan_ray_count", scanRays.size());
        vision.addProperty("scan_distance", distance);
        vision.addProperty("scan_fov", fov);

        JsonArray visibleEntities = getVisibleEntities(mc, Math.min(distance, config.nearbyEntityRadius), MAX_VISIBLE_ENTITIES);
        vision.add("visible_entities", visibleEntities);
        vision.addProperty("visible_entity_count", visibleEntities.size());

        JsonObject focus = describeFocusedEntity(mc, yaw, pitch, distance);
        vision.add("focused_entity", focus);

        return vision;
    }

    private JsonObject buildThreatAssessment(Minecraft mc) {
        return buildThreatAssessment(mc, null);
    }

    private JsonObject buildThreatAssessment(Minecraft mc, JsonObject cmd) {
        double radius = clampDouble(getDouble(cmd, "radius", config.threatAssessmentRadius),
                2.0, Math.max(config.threatAssessmentRadius, MAX_SCAN_RADIUS));
        Vec3 playerPos = mc.player.position();
        AABB box = new AABB(
                playerPos.x - radius, playerPos.y - radius, playerPos.z - radius,
                playerPos.x + radius, playerPos.y + radius, playerPos.z + radius
        );

        List<Entity> threats = new ArrayList<>();
        for (Entity entity : mc.level.getEntities(mc.player, box)) {
            if (isThreatEntity(entity)) {
                threats.add(entity);
            }
        }

        threats.sort((a, b) -> Double.compare(scoreThreat(mc, b), scoreThreat(mc, a)));

        JsonArray threatArray = new JsonArray();
        double totalScore = 0.0;
        double nearestDistance = Double.MAX_VALUE;
        Entity primaryThreat = null;

        for (Entity threat : threats) {
            double score = scoreThreat(mc, threat);
            double distance = playerPos.distanceTo(threat.position());
            totalScore += score;
            nearestDistance = Math.min(nearestDistance, distance);
            if (primaryThreat == null) {
                primaryThreat = threat;
            }
            if (threatArray.size() < MAX_THREATS) {
                JsonObject threatObj = buildEntityState(mc, threat, playerPos);
                threatObj.addProperty("threat_score", score);
                threatObj.addProperty("immediate", distance <= 4.0 || score >= 12.0);
                threatArray.add(threatObj);
            }
        }

        JsonArray hazards = scanHazardBlocks(mc, getInt(cmd, "hazard_radius", 6));

        JsonObject assessment = new JsonObject();
        assessment.add("entities", threatArray);
        assessment.add("hazards", hazards);
        assessment.addProperty("entity_count", threats.size());
        assessment.addProperty("hazard_count", hazards.size());
        assessment.addProperty("score", totalScore);
        assessment.addProperty("nearest_distance", nearestDistance == Double.MAX_VALUE ? -1.0 : nearestDistance);
        assessment.addProperty("danger_level", classifyDanger(mc, totalScore, nearestDistance, hazards.size()));
        assessment.addProperty("safe_escape_yaw", computeEscapeYaw(mc, threats));
        assessment.addProperty("should_flee", shouldFlee(mc, totalScore, nearestDistance));

        if (primaryThreat != null) {
            assessment.add("primary", buildEntityState(mc, primaryThreat, playerPos));
        }

        return assessment;
    }

    private JsonObject buildSurvivalState(Minecraft mc) {
        LocalPlayer player = mc.player;
        JsonObject survival = new JsonObject();

        float health = player.getHealth();
        int food = player.getFoodData().getFoodLevel();
        float saturation = player.getFoodData().getSaturationLevel();
        boolean inImmediateDanger = player.isInLava() || player.isOnFire() || player.fallDistance > 6.0f;
        JsonObject threats = buildThreatAssessment(mc);

        survival.addProperty("health_bucket", healthBucket(health, player.getMaxHealth()));
        survival.addProperty("food_bucket", foodBucket(food));
        survival.addProperty("oxygen_bucket", oxygenBucket(player.getAirSupply(), player.getMaxAirSupply()));
        survival.addProperty("should_eat", food <= 14 || health <= player.getMaxHealth() * 0.55f);
        survival.addProperty("should_flee", threats.get("should_flee").getAsBoolean());
        survival.addProperty("should_seek_light",
                mc.level.isNight() && mc.level.getMaxLocalRawBrightness(player.blockPosition()) < 8);
        survival.addProperty("should_surface", player.isUnderWater() && player.getAirSupply() < player.getMaxAirSupply() * 0.45);
        survival.addProperty("should_extinguish", player.isOnFire() || player.isInLava());
        survival.addProperty("fall_risk", player.fallDistance);
        survival.addProperty("immediate_danger", inImmediateDanger);
        survival.addProperty("regeneration_possible", food >= 18 && saturation > 0.0f);

        int bestFood = findBestFoodSlot(mc, true);
        survival.addProperty("best_food_hotbar_slot", bestFood);
        survival.addProperty("has_hotbar_food", bestFood >= 0);
        survival.addProperty("best_weapon_hotbar_slot", findBestWeaponSlot(mc, true));
        survival.addProperty("best_weapon_inventory_slot", findBestWeaponSlot(mc, false));

        JsonArray hazards = scanHazardBlocks(mc, 5);
        survival.add("nearby_hazards", hazards);
        survival.addProperty("nearby_hazard_count", hazards.size());
        survival.addProperty("recommended_action", recommendSurvivalAction(mc, threats, bestFood));

        return survival;
    }

    private JsonObject buildCombatState(Minecraft mc) {
        JsonObject combat = new JsonObject();
        Entity bestTarget = findBestCombatTarget(mc, config.autoAttackRadius);
        ItemStack weapon = mc.player.getMainHandItem();
        String weaponId = weapon.isEmpty() ? "empty" : BuiltInRegistries.ITEM.getKey(weapon.getItem()).toString();

        combat.addProperty("weapon", weaponId);
        combat.addProperty("weapon_score", scoreWeapon(weapon));
        combat.addProperty("shield_available", hasShield(mc));
        combat.addProperty("attack_strength", mc.player.getAttackStrengthScale(0.0f));
        combat.addProperty("health_margin", mc.player.getHealth() / Math.max(1.0f, mc.player.getMaxHealth()));
        combat.addProperty("best_weapon_hotbar_slot", findBestWeaponSlot(mc, true));

        if (bestTarget != null) {
            Vec3 playerPos = mc.player.position();
            double distance = playerPos.distanceTo(bestTarget.position());
            boolean visible = hasLineOfSight(mc, bestTarget);
            combat.add("target", buildEntityState(mc, bestTarget, playerPos));
            combat.addProperty("has_target", true);
            combat.addProperty("can_attack_now", visible && distance <= config.combatReachDistance
                    && mc.player.getAttackStrengthScale(0.0f) >= 0.75f);
            combat.addProperty("recommended_action", combatRecommendation(mc, bestTarget, visible, distance));
        } else {
            combat.addProperty("has_target", false);
            combat.addProperty("can_attack_now", false);
            combat.addProperty("recommended_action", "scan");
        }

        return combat;
    }

    private JsonArray buildNearbyEntities(Minecraft mc) {
        JsonArray entities = new JsonArray();
        Vec3 playerPos = mc.player.position();
        double radius = config.nearbyEntityRadius;

        AABB box = new AABB(
                playerPos.x - radius, playerPos.y - radius, playerPos.z - radius,
                playerPos.x + radius, playerPos.y + radius, playerPos.z + radius
        );

        int count = 0;
        for (Entity entity : mc.level.getEntities(mc.player, box)) {
            if (count++ >= MAX_NEARBY_ENTITIES) break;
            entities.add(buildEntityState(mc, entity, playerPos));
        }

        return entities;
    }

    private JsonArray buildNearbyBlocks(Minecraft mc) {
        JsonArray blocks = new JsonArray();
        BlockPos playerPos = mc.player.blockPosition();
        int radius = config.nearbyBlockRadius;
        ClientLevel level = mc.level;
        int density = config.blockSampleDensity;

        int count = 0;
        for (int shell = 1; shell <= radius && count < MAX_NEARBY_BLOCKS; shell++) {
            for (int dx = -shell; dx <= shell; dx += density) {
                for (int dy = -shell; dy <= shell; dy += density) {
                    for (int dz = -shell; dz <= shell; dz += density) {
                        if (Math.abs(dx) == shell || Math.abs(dy) == shell || Math.abs(dz) == shell) {
                            if (count++ >= MAX_NEARBY_BLOCKS) break;

                            BlockPos pos = playerPos.offset(dx, dy, dz);
                            BlockState state = level.getBlockState(pos);

                            if (!state.isAir()) {
                                blocks.add(buildBlockStateObject(mc, pos, state, Math.sqrt(dx * dx + dy * dy + dz * dz)));
                            }
                        }
                    }
                }
            }
        }

        return blocks;
    }

    private JsonObject describeRaycast(Minecraft mc, float yaw, float pitch, double distance) {
        double clampedDistance = clampDouble(distance, 0.5, config.maxRaycastDistance);
        Vec3 start = mc.player.getEyePosition();
        Vec3 lookVec = Vec3.directionFromRotation(pitch, yaw);
        Vec3 end = start.add(lookVec.scale(clampedDistance));

        BlockHitResult blockHit = mc.level.clip(new ClipContext(
                start, end, ClipContext.Block.OUTLINE, ClipContext.Fluid.NONE, mc.player
        ));
        double blockDistance = blockHit.getType() == HitResult.Type.MISS
                ? clampedDistance
                : start.distanceTo(blockHit.getLocation());

        Entity entityHit = findEntityAlongRay(mc, start, lookVec, clampedDistance, blockDistance);

        JsonObject response = new JsonObject();
        response.addProperty("yaw", yaw);
        response.addProperty("pitch", pitch);
        response.addProperty("distance", clampedDistance);

        if (entityHit != null) {
            Vec3 entityPos = targetPoint(entityHit);
            response.addProperty("hit", true);
            response.addProperty("hit_type", "entity");
            response.addProperty("entity_id", entityHit.getId());
            response.addProperty("entity_type", entityId(entityHit));
            response.addProperty("x", entityPos.x);
            response.addProperty("y", entityPos.y);
            response.addProperty("z", entityPos.z);
            response.addProperty("hit_distance", start.distanceTo(entityPos));
            response.addProperty("visible", hasLineOfSight(mc, entityHit));
            return response;
        }

        response.addProperty("hit", blockHit.getType() != HitResult.Type.MISS);
        if (blockHit.getType() == HitResult.Type.BLOCK) {
            BlockPos pos = blockHit.getBlockPos();
            BlockState state = mc.level.getBlockState(pos);
            response.addProperty("hit_type", "block");
            response.addProperty("x", pos.getX());
            response.addProperty("y", pos.getY());
            response.addProperty("z", pos.getZ());
            response.addProperty("block", blockId(state));
            response.addProperty("face", blockHit.getDirection().toString());
            response.addProperty("hit_distance", blockDistance);
            response.addProperty("hazard", isHazardBlock(state));
            response.addProperty("resource", isResourceBlock(state));
        } else {
            response.addProperty("hit_type", "miss");
            response.addProperty("hit_distance", clampedDistance);
        }
        return response;
    }

    private Entity findEntityAlongRay(Minecraft mc, Vec3 start, Vec3 direction, double distance, double blockDistance) {
        Entity best = null;
        double bestAlong = Double.MAX_VALUE;
        AABB searchBox = mc.player.getBoundingBox().inflate(distance);

        for (Entity entity : mc.level.getEntities(mc.player, searchBox)) {
            if (!entity.isAlive()) {
                continue;
            }
            Vec3 toTarget = targetPoint(entity).subtract(start);
            double along = toTarget.dot(direction);
            if (along < 0.0 || along > distance || along > blockDistance + 0.15) {
                continue;
            }

            double perpendicularSq = Math.max(0.0, toTarget.lengthSqr() - along * along);
            double hitRadius = entity instanceof ItemEntity ? 0.45 : 0.8;
            if (perpendicularSq <= hitRadius * hitRadius && along < bestAlong) {
                bestAlong = along;
                best = entity;
            }
        }

        return best;
    }

    private JsonObject describeFocusedEntity(Minecraft mc, float yaw, float pitch, double distance) {
        Vec3 start = mc.player.getEyePosition();
        Vec3 lookVec = Vec3.directionFromRotation(pitch, yaw);
        Entity target = findEntityAlongRay(mc, start, lookVec, distance, distance);
        JsonObject focused = new JsonObject();
        if (target == null) {
            focused.addProperty("has_entity", false);
            return focused;
        }
        focused.addProperty("has_entity", true);
        focused.add("entity", buildEntityState(mc, target, mc.player.position()));
        return focused;
    }

    private JsonArray getVisibleEntities(Minecraft mc, double radius, int limit) {
        JsonArray visible = new JsonArray();
        Vec3 playerPos = mc.player.position();
        AABB box = new AABB(
                playerPos.x - radius, playerPos.y - radius, playerPos.z - radius,
                playerPos.x + radius, playerPos.y + radius, playerPos.z + radius
        );

        List<Entity> candidates = new ArrayList<>(mc.level.getEntities(mc.player, box));
        candidates.sort(Comparator.comparingDouble(entity -> playerPos.distanceTo(entity.position())));

        int count = 0;
        for (Entity entity : candidates) {
            if (count >= limit) {
                break;
            }
            if (hasLineOfSight(mc, entity) && isWithinLooseFov(mc, entity, 115.0)) {
                visible.add(buildEntityState(mc, entity, playerPos));
                count++;
            }
        }
        return visible;
    }

    private JsonObject buildEntityState(Minecraft mc, Entity entity, Vec3 playerPos) {
        JsonObject entityObj = new JsonObject();
        Vec3 pos = entity.position();
        Vec3 target = targetPoint(entity);
        double[] yawPitch = yawPitch(mc.player.getEyePosition(), target);
        Vec3 motion = entity.getDeltaMovement();
        double distance = playerPos.distanceTo(pos);

        entityObj.addProperty("type", entityId(entity));
        entityObj.addProperty("id", entity.getId());
        entityObj.addProperty("uuid", entity.getUUID().toString());
        entityObj.addProperty("x", pos.x);
        entityObj.addProperty("y", pos.y);
        entityObj.addProperty("z", pos.z);
        entityObj.addProperty("distance", distance);
        entityObj.addProperty("yaw_to", yawPitch[0]);
        entityObj.addProperty("pitch_to", yawPitch[1]);
        entityObj.addProperty("angle_from_crosshair", angleFromCrosshair(mc, entity));
        entityObj.addProperty("line_of_sight", hasLineOfSight(mc, entity));
        entityObj.addProperty("hostile", entity instanceof Monster);
        entityObj.addProperty("threat", isThreatEntity(entity));
        entityObj.addProperty("projectile", entity instanceof Projectile);
        entityObj.addProperty("item", entity instanceof ItemEntity);
        entityObj.addProperty("combat_score", scoreCombatTarget(mc, entity));
        entityObj.addProperty("threat_score", scoreThreat(mc, entity));

        if (entity instanceof ItemEntity itemEntity) {
            ItemStack stack = itemEntity.getItem();
            entityObj.addProperty("item_name", stack.isEmpty() ? "empty" :
                    BuiltInRegistries.ITEM.getKey(stack.getItem()).toString());
            entityObj.addProperty("item_count", stack.getCount());
        }

        if (entity instanceof LivingEntity living) {
            entityObj.addProperty("health", living.getHealth());
            entityObj.addProperty("max_health", living.getMaxHealth());
            entityObj.addProperty("is_alive", living.isAlive());
            entityObj.addProperty("armor", living.getArmorValue());
        }

        entityObj.addProperty("velocity", Math.sqrt(motion.x * motion.x + motion.y * motion.y + motion.z * motion.z));
        entityObj.addProperty("velocity_x", motion.x);
        entityObj.addProperty("velocity_y", motion.y);
        entityObj.addProperty("velocity_z", motion.z);
        entityObj.addProperty("moving_toward_player", isMovingTowardPlayer(entity, mc.player));
        entityObj.addProperty("is_on_fire", entity.isOnFire());
        entityObj.addProperty("is_in_water", entity.isInWater());
        entityObj.addProperty("is_on_ground", entity.onGround());

        return entityObj;
    }

    private JsonObject buildBlockStateObject(Minecraft mc, BlockPos pos, BlockState state, double distance) {
        JsonObject blockObj = new JsonObject();
        blockObj.addProperty("x", pos.getX());
        blockObj.addProperty("y", pos.getY());
        blockObj.addProperty("z", pos.getZ());
        blockObj.addProperty("block", blockId(state));
        blockObj.addProperty("distance", distance);
        blockObj.addProperty("hardness", state.getDestroySpeed(mc.level, pos));
        blockObj.addProperty("block_light", mc.level.getBrightness(LightLayer.BLOCK, pos));
        blockObj.addProperty("sky_light", mc.level.getBrightness(LightLayer.SKY, pos));
        blockObj.addProperty("can_see_sky", mc.level.canSeeSky(pos));
        blockObj.addProperty("is_log", state.is(BlockTags.LOGS));
        blockObj.addProperty("is_leaves", state.is(BlockTags.LEAVES));
        blockObj.addProperty("is_fluid", !state.getFluidState().isEmpty());
        blockObj.addProperty("hazard", isHazardBlock(state));
        blockObj.addProperty("resource", isResourceBlock(state));
        blockObj.addProperty("best_tool_hotbar_slot", findBestToolSlotForBlock(mc, state, true));
        return blockObj;
    }

    private JsonArray scanHazardBlocks(Minecraft mc, int requestedRadius) {
        int radius = Math.max(1, Math.min(10, requestedRadius));
        JsonArray hazards = new JsonArray();
        BlockPos playerPos = mc.player.blockPosition();

        for (int dx = -radius; dx <= radius && hazards.size() < MAX_HAZARDS; dx++) {
            for (int dy = -2; dy <= 3 && hazards.size() < MAX_HAZARDS; dy++) {
                for (int dz = -radius; dz <= radius && hazards.size() < MAX_HAZARDS; dz++) {
                    BlockPos pos = playerPos.offset(dx, dy, dz);
                    BlockState state = mc.level.getBlockState(pos);
                    if (isHazardBlock(state)) {
                        hazards.add(buildBlockStateObject(mc, pos, state, Math.sqrt(dx * dx + dy * dy + dz * dz)));
                    }
                }
            }
        }

        return hazards;
    }

    private boolean hasLineOfSight(Minecraft mc, Entity entity) {
        Vec3 start = mc.player.getEyePosition();
        Vec3 target = targetPoint(entity);
        BlockHitResult hit = mc.level.clip(new ClipContext(
                start, target, ClipContext.Block.COLLIDER, ClipContext.Fluid.NONE, mc.player
        ));
        return hit.getType() == HitResult.Type.MISS || start.distanceTo(hit.getLocation()) + 0.35 >= start.distanceTo(target);
    }

    private boolean isWithinLooseFov(Minecraft mc, Entity entity, double fov) {
        return angleFromCrosshair(mc, entity) <= fov / 2.0;
    }

    private boolean isMovingTowardPlayer(Entity entity, LocalPlayer player) {
        Vec3 motion = entity.getDeltaMovement();
        if (motion.lengthSqr() < 0.0001) {
            return false;
        }
        Vec3 toPlayer = player.position().subtract(entity.position()).normalize();
        return motion.normalize().dot(toPlayer) > 0.45;
    }

    private Entity findBestCombatTarget(Minecraft mc, double radius) {
        Vec3 playerPos = mc.player.position();
        AABB box = new AABB(
                playerPos.x - radius, playerPos.y - radius, playerPos.z - radius,
                playerPos.x + radius, playerPos.y + radius, playerPos.z + radius
        );

        Entity best = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (Entity entity : mc.level.getEntities(mc.player, box)) {
            if (!(entity instanceof LivingEntity living) || !living.isAlive()) {
                continue;
            }
            if (!isThreatEntity(entity)) {
                continue;
            }
            double score = scoreCombatTarget(mc, entity);
            if (score > bestScore) {
                bestScore = score;
                best = entity;
            }
        }
        return best;
    }

    private boolean isThreatEntity(Entity entity) {
        String type = entityId(entity);
        return entity instanceof Monster
                || entity instanceof Projectile
                || type.contains("creeper")
                || type.contains("slime")
                || type.contains("phantom")
                || type.contains("drowned")
                || type.contains("pillager")
                || type.contains("vindicator")
                || type.contains("ravager")
                || type.contains("witch");
    }

    private double scoreThreat(Minecraft mc, Entity entity) {
        double distance = Math.max(0.1, mc.player.position().distanceTo(entity.position()));
        double score = 0.0;
        String type = entityId(entity);

        if (entity instanceof Monster) score += 7.0;
        if (entity instanceof Projectile) score += 9.0;
        if (type.contains("creeper")) score += 7.0;
        if (type.contains("skeleton") || type.contains("witch") || type.contains("pillager")) score += 4.0;
        if (type.contains("phantom")) score += 3.0;
        if (entity.isOnFire()) score += 0.5;
        if (hasLineOfSight(mc, entity)) score += 2.0;
        if (isMovingTowardPlayer(entity, mc.player)) score += 2.5;

        score += Math.max(0.0, (16.0 - distance) * 0.45);
        if (distance <= 3.0) score += 5.0;

        return score;
    }

    private double scoreCombatTarget(Minecraft mc, Entity entity) {
        double distance = Math.max(0.1, mc.player.position().distanceTo(entity.position()));
        double score = scoreThreat(mc, entity) - distance * 0.35;
        if (distance <= config.combatReachDistance) {
            score += 4.0;
        }
        if (hasLineOfSight(mc, entity)) {
            score += 2.0;
        }
        if (entity instanceof LivingEntity living) {
            score += Math.max(0.0, 6.0 - living.getHealth() * 0.1);
        }
        return score;
    }

    private String classifyDanger(Minecraft mc, double score, double nearestDistance, int hazardCount) {
        if (mc.player.isInLava() || mc.player.isOnFire() || mc.player.getHealth() <= 4.0f) {
            return "critical";
        }
        if (nearestDistance <= 3.0 || score >= 18.0) {
            return "high";
        }
        if (nearestDistance <= 8.0 || score >= 9.0 || hazardCount > 0) {
            return "moderate";
        }
        if (score > 0.0) {
            return "low";
        }
        return "none";
    }

    private boolean shouldFlee(Minecraft mc, double score, double nearestDistance) {
        return mc.player.getHealth() <= 7.0f
                || mc.player.isInLava()
                || mc.player.isOnFire()
                || nearestDistance <= 3.0
                || score >= 18.0;
    }

    private double computeEscapeYaw(Minecraft mc, List<Entity> threats) {
        Vec3 playerPos = mc.player.position();
        double awayX = 0.0;
        double awayZ = 0.0;

        for (Entity threat : threats) {
            Vec3 away = playerPos.subtract(threat.position());
            double distance = Math.max(1.0, away.length());
            awayX += away.x / (distance * distance);
            awayZ += away.z / (distance * distance);
        }

        if (Math.abs(awayX) + Math.abs(awayZ) < 0.0001) {
            Vec3 look = Vec3.directionFromRotation(0.0f, mc.player.getYRot());
            awayX = look.x;
            awayZ = look.z;
        }

        return Math.toDegrees(Math.atan2(-awayX, awayZ));
    }

    private String recommendSurvivalAction(Minecraft mc, JsonObject threats, int bestFoodSlot) {
        if (mc.player.isInLava() || mc.player.isOnFire()) {
            return "escape_hazard";
        }
        if (mc.player.isUnderWater() && mc.player.getAirSupply() < mc.player.getMaxAirSupply() * 0.45) {
            return "surface";
        }
        if (threats.get("should_flee").getAsBoolean()) {
            return "flee";
        }
        if ((mc.player.getFoodData().getFoodLevel() <= 14 || mc.player.getHealth() <= 10.0f) && bestFoodSlot >= 0) {
            return "eat";
        }
        if (mc.level.isNight() && mc.level.getMaxLocalRawBrightness(mc.player.blockPosition()) < 8) {
            return "seek_light";
        }
        return "continue";
    }

    private String combatRecommendation(Minecraft mc, Entity target, boolean visible, double distance) {
        if (mc.player.getHealth() <= 7.0f && distance <= 6.0) {
            return "flee";
        }
        if (!visible) {
            return "reposition";
        }
        if (distance <= config.combatReachDistance && mc.player.getAttackStrengthScale(0.0f) >= 0.75f) {
            return "attack";
        }
        if (distance <= 7.0) {
            return hasShield(mc) ? "block_or_kite" : "kite";
        }
        return "approach";
    }

    private boolean isHazardBlock(BlockState state) {
        String id = blockId(state);
        return id.contains("lava")
                || id.contains("fire")
                || id.contains("magma_block")
                || id.contains("cactus")
                || id.contains("powder_snow")
                || id.contains("sweet_berry_bush");
    }

    private boolean isResourceBlock(BlockState state) {
        String id = blockId(state);
        return state.is(BlockTags.LOGS)
                || id.contains("ore")
                || id.contains("coal")
                || id.contains("iron")
                || id.contains("copper")
                || id.contains("diamond")
                || id.contains("redstone")
                || id.contains("lapis")
                || id.contains("gold")
                || id.contains("emerald");
    }

    private int findBestFoodSlot(Minecraft mc, boolean hotbarOnly) {
        Inventory inv = mc.player.getInventory();
        int end = hotbarOnly ? 9 : inv.getContainerSize();
        int bestSlot = -1;
        int bestScore = Integer.MIN_VALUE;
        for (int i = 0; i < end; i++) {
            ItemStack stack = inv.getItem(i);
            FoodProperties food = resolveFoodProperties(stack, mc.player);
            if (food == null) {
                continue;
            }
            int score = food.nutrition() * 10 + Math.round(food.saturation() * 10.0f);
            if (score > bestScore) {
                bestScore = score;
                bestSlot = i;
            }
        }
        return bestSlot;
    }

    private int findBestWeaponSlot(Minecraft mc, boolean hotbarOnly) {
        Inventory inv = mc.player.getInventory();
        int end = hotbarOnly ? 9 : inv.getContainerSize();
        int bestSlot = -1;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < end; i++) {
            ItemStack stack = inv.getItem(i);
            double score = scoreWeapon(stack);
            if (score > bestScore) {
                bestScore = score;
                bestSlot = i;
            }
        }
        return bestScore > 0.0 ? bestSlot : -1;
    }

    private int findBestToolSlotForBlock(Minecraft mc, BlockState state, boolean hotbarOnly) {
        Inventory inv = mc.player.getInventory();
        int end = hotbarOnly ? 9 : inv.getContainerSize();
        int bestSlot = -1;
        double bestScore = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < end; i++) {
            ItemStack stack = inv.getItem(i);
            if (stack.isEmpty()) {
                continue;
            }
            double score = scoreToolForBlock(stack, state);
            if (score > bestScore) {
                bestScore = score;
                bestSlot = i;
            }
        }
        return bestScore > 1.0 ? bestSlot : -1;
    }

    private double scoreWeapon(ItemStack stack) {
        if (stack.isEmpty()) {
            return 0.0;
        }
        String id = BuiltInRegistries.ITEM.getKey(stack.getItem()).toString();
        double score = 0.0;
        if (id.contains("sword")) score += 6.0;
        if (id.contains("axe")) score += 5.0;
        if (id.contains("trident")) score += 6.5;
        if (id.contains("bow") || id.contains("crossbow")) score += 4.0;
        if (id.contains("netherite")) score += 4.0;
        if (id.contains("diamond")) score += 3.0;
        if (id.contains("iron")) score += 2.0;
        if (id.contains("stone")) score += 1.0;
        if (id.contains("wooden") || id.contains("golden")) score += 0.5;
        if (stack.isDamageableItem()) {
            score += Math.max(0.0, 1.0 - (double) stack.getDamageValue() / Math.max(1, stack.getMaxDamage()));
        }
        return score;
    }

    private double scoreToolForBlock(ItemStack stack, BlockState state) {
        double speed = Math.max(0.0, stack.getDestroySpeed(state));
        String id = BuiltInRegistries.ITEM.getKey(stack.getItem()).toString();
        boolean toolLike = id.contains("pickaxe")
                || id.contains("axe")
                || id.contains("shovel")
                || id.contains("hoe")
                || id.contains("shears");
        if (!toolLike && !stack.isCorrectToolForDrops(state)) {
            return speed;
        }
        if (toolLike && speed <= 1.0 && !stack.isCorrectToolForDrops(state)) {
            return speed;
        }
        double score = speed;
        if (stack.isCorrectToolForDrops(state)) {
            score += 10.0;
        }
        if (id.contains("netherite")) score += 4.0;
        if (id.contains("diamond")) score += 3.0;
        if (id.contains("iron")) score += 2.0;
        if (id.contains("stone")) score += 1.0;
        if (stack.isDamageableItem()) {
            score += Math.max(0.0, 1.0 - (double) stack.getDamageValue() / Math.max(1, stack.getMaxDamage()));
        }
        return score;
    }

    private boolean hasShield(Minecraft mc) {
        String main = mc.player.getMainHandItem().isEmpty()
                ? ""
                : BuiltInRegistries.ITEM.getKey(mc.player.getMainHandItem().getItem()).toString();
        String off = mc.player.getOffhandItem().isEmpty()
                ? ""
                : BuiltInRegistries.ITEM.getKey(mc.player.getOffhandItem().getItem()).toString();
        return main.contains("shield") || off.contains("shield");
    }

    private String healthBucket(float health, float maxHealth) {
        float ratio = health / Math.max(1.0f, maxHealth);
        if (ratio <= 0.2f) return "critical";
        if (ratio <= 0.5f) return "low";
        if (ratio <= 0.85f) return "hurt";
        return "healthy";
    }

    private String foodBucket(int food) {
        if (food <= 6) return "starving";
        if (food <= 14) return "hungry";
        if (food <= 18) return "stable";
        return "full";
    }

    private String oxygenBucket(int air, int maxAir) {
        double ratio = (double) air / Math.max(1, maxAir);
        if (ratio <= 0.2) return "critical";
        if (ratio <= 0.5) return "low";
        if (ratio < 1.0) return "recovering";
        return "full";
    }

    private double angleFromCrosshair(Minecraft mc, Entity entity) {
        double[] targetAngles = yawPitch(mc.player.getEyePosition(), targetPoint(entity));
        double yawDelta = Math.abs(angleDeltaDegrees(mc.player.getYRot(), targetAngles[0]));
        double pitchDelta = Math.abs(angleDeltaDegrees(mc.player.getXRot(), targetAngles[1]));
        return Math.sqrt(yawDelta * yawDelta + pitchDelta * pitchDelta);
    }

    private Vec3 targetPoint(Entity entity) {
        return entity.getBoundingBox().getCenter();
    }

    private double[] yawPitch(Vec3 origin, Vec3 target) {
        double dx = target.x - origin.x;
        double dy = target.y - origin.y;
        double dz = target.z - origin.z;
        double horizontal = Math.sqrt(dx * dx + dz * dz);
        double yaw = Math.toDegrees(Math.atan2(-dx, dz));
        double pitch = Math.toDegrees(Math.atan2(-dy, horizontal));
        return new double[]{yaw, clampDouble(pitch, -90.0, 90.0)};
    }

    private double angleDeltaDegrees(double from, double to) {
        double delta = (to - from) % 360.0;
        if (delta >= 180.0) delta -= 360.0;
        if (delta < -180.0) delta += 360.0;
        return delta;
    }

    private String entityId(Entity entity) {
        return BuiltInRegistries.ENTITY_TYPE.getKey(entity.getType()).toString();
    }

    private String blockId(BlockState state) {
        return BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString();
    }

    private int getInt(JsonObject cmd, String key, int defaultValue) {
        if (cmd == null || !cmd.has(key)) {
            return defaultValue;
        }
        try {
            return cmd.get(key).getAsInt();
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private float getFloat(JsonObject cmd, String key, float defaultValue) {
        if (cmd == null || !cmd.has(key)) {
            return defaultValue;
        }
        try {
            return cmd.get(key).getAsFloat();
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private double getDouble(JsonObject cmd, String key, double defaultValue) {
        if (cmd == null || !cmd.has(key)) {
            return defaultValue;
        }
        try {
            return cmd.get(key).getAsDouble();
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private boolean getBoolean(JsonObject cmd, String key, boolean defaultValue) {
        if (cmd == null || !cmd.has(key)) {
            return defaultValue;
        }
        try {
            return cmd.get(key).getAsBoolean();
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private String getString(JsonObject cmd, String key, String defaultValue) {
        if (cmd == null || !cmd.has(key)) {
            return defaultValue;
        }
        try {
            return cmd.get(key).getAsString();
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private Direction parseDirection(JsonObject cmd, String key, Direction defaultValue) {
        if (cmd == null || !cmd.has(key)) {
            return defaultValue;
        }
        try {
            return Direction.valueOf(cmd.get(key).getAsString().toUpperCase(Locale.ROOT));
        } catch (RuntimeException e) {
            return defaultValue;
        }
    }

    private float clampFloat(float value, float min, float max) {
        return Math.max(min, Math.min(max, value));
    }

    private double clampDouble(double value, double min, double max) {
        return Math.max(min, Math.min(max, value));
    }

    private int clampInt(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }

    private void updateMiningProgress(Minecraft mc) {
        if (mc.gameMode == null || mc.player == null || mc.level == null) return;

        activeMining.entrySet().removeIf(entry -> {
            BlockPos pos = entry.getKey();
            MiningState state = entry.getValue();
            BlockState blockState = mc.level.getBlockState(pos);

            if (blockState.isAir()) {
                state.completed = true;
                state.progress = 1.0f;
                return true;
            }

            double reach = Math.max(config.maxReachDistance, MAX_REACH_DISTANCE) + 0.75;
            if (mc.player.position().distanceTo(Vec3.atCenterOf(pos)) > reach) {
                return true;
            }

            float hardness = blockState.getDestroySpeed(mc.level, pos);
            if (hardness < 0.0f) {
                return true;
            }

            float toolSpeed = Math.max(0.1f, mc.player.getMainHandItem().getDestroySpeed(blockState));
            long elapsedTicks = Math.max(1L, mc.level.getGameTime() - state.startTick);
            float estimatedTicks = Math.max(1.0f, hardness * 30.0f / toolSpeed);
            state.progress = Math.min(0.99f, elapsedTicks / estimatedTicks);

            mc.gameMode.continueDestroyBlock(pos, state.direction);
            mc.player.swing(InteractionHand.MAIN_HAND);
            return state.completed;
        });
    }

    private void applyControlState(Minecraft mc) {
        if (mc.player == null) {
            controlState.clearMovement();
            return;
        }

        long now = System.currentTimeMillis();
        if (controlState.expiresAtMs > 0 && now >= controlState.expiresAtMs) {
            controlState.clearMovement();
        }

        mc.player.input.forwardImpulse = controlState.forward;
        mc.player.input.leftImpulse = controlState.strafe;
        mc.player.input.up = controlState.forward > 0.0f;
        mc.player.input.down = controlState.forward < 0.0f;
        mc.player.input.left = controlState.strafe > 0.0f;
        mc.player.input.right = controlState.strafe < 0.0f;
        mc.player.input.shiftKeyDown = controlState.sneaking;
        mc.options.keyUp.setDown(controlState.forward > 0.0f);
        mc.options.keyDown.setDown(controlState.forward < 0.0f);
        mc.options.keyLeft.setDown(controlState.strafe > 0.0f);
        mc.options.keyRight.setDown(controlState.strafe < 0.0f);
        mc.options.keyShift.setDown(controlState.sneaking);
        mc.player.setSprinting(controlState.sprinting && controlState.forward > 0.0f);
        mc.player.setShiftKeyDown(controlState.sneaking);

        applyMovementVelocity(mc);

        if (controlState.swimming && mc.player.isInWater()) {
            mc.player.setSwimming(true);
        }
    }

    private void applyMovementVelocity(Minecraft mc) {
        double forward = controlState.forward;
        double strafe = controlState.strafe;
        if (Math.abs(forward) < 0.001 && Math.abs(strafe) < 0.001) {
            return;
        }

        double magnitude = Math.sqrt(forward * forward + strafe * strafe);
        if (magnitude > 1.0) {
            forward /= magnitude;
            strafe /= magnitude;
        }

        double yaw = Math.toRadians(mc.player.getYRot());
        double x;
        double z;
        if (controlState.useWorldMovement) {
            x = controlState.worldMovementX;
            z = controlState.worldMovementZ;
        } else {
            x = -Math.sin(yaw) * forward + Math.cos(yaw) * strafe;
            z = Math.cos(yaw) * forward + Math.sin(yaw) * strafe;
        }
        double horizontalMagnitude = Math.sqrt(x * x + z * z);
        if (horizontalMagnitude > 1.0) {
            x /= horizontalMagnitude;
            z /= horizontalMagnitude;
        }
        double speed = controlState.sprinting && forward > 0.0 ? 0.22 : 0.14;
        if (controlState.sneaking) {
            speed *= 0.35;
        }
        if (mc.player.isInWater()) {
            speed *= 0.55;
        }

        Vec3 motion = mc.player.getDeltaMovement();
        Vec3 step = new Vec3(x * speed, 0.0, z * speed);
        mc.player.move(MoverType.SELF, step);
        mc.player.setDeltaMovement(x * speed, motion.y, z * speed);
        mc.player.connection.send(new ServerboundMovePlayerPacket.PosRot(
                mc.player.getX(),
                mc.player.getY(),
                mc.player.getZ(),
                mc.player.getYRot(),
                mc.player.getXRot(),
                mc.player.onGround()
        ));
        mc.player.hasImpulse = true;
    }

    private void updateNavigation(Minecraft mc) {
        NavigationState nav = navigationState;
        if (nav == null || mc.player == null || mc.level == null) {
            return;
        }

        long now = System.currentTimeMillis();
        Vec3 playerPos = mc.player.position();
        double distance = playerPos.distanceTo(nav.target);

        if (distance <= nav.stopDistance || now - nav.startedAtMs > nav.timeoutMs) {
            navigationState = null;
            controlState.clearMovement();
            return;
        }

        Vec3 direction = nav.target.subtract(playerPos);
        double horizontal = Math.sqrt(direction.x * direction.x + direction.z * direction.z);
        if (horizontal < 0.001) {
            navigationState = null;
            controlState.clearMovement();
            return;
        }

        double yaw = Math.toDegrees(Math.atan2(-direction.x, direction.z));
        double pitch = Math.toDegrees(Math.atan2(-direction.y, horizontal));
        mc.player.setYRot((float) yaw);
        mc.player.setXRot((float) clampDouble(pitch, -45.0, 45.0));

        controlState.forward = 1.0f;
        controlState.strafe = 0.0f;
        controlState.sprinting = nav.sprint && distance > 4.0;
        controlState.useWorldMovement = true;
        controlState.worldMovementX = direction.x / horizontal;
        controlState.worldMovementZ = direction.z / horizontal;
        controlState.expiresAtMs = 0;

        if (nav.lastPos == null || now - nav.lastProgressCheckMs >= 500L) {
            if (nav.lastPos != null && playerPos.distanceTo(nav.lastPos) < 0.08) {
                nav.stuckChecks++;
            } else {
                nav.stuckChecks = 0;
            }
            nav.lastPos = playerPos;
            nav.lastProgressCheckMs = now;
        }

        if ((mc.player.horizontalCollision || nav.stuckChecks >= 2 || isStepBlocked(mc)) && mc.player.onGround()) {
            mc.player.jumpFromGround();
        }
    }

    private boolean isStepBlocked(Minecraft mc) {
        Direction facing = Direction.fromYRot(mc.player.getYRot());
        BlockPos front = mc.player.blockPosition().relative(facing);
        BlockState feet = mc.level.getBlockState(front);
        BlockState head = mc.level.getBlockState(front.above());
        return !feet.getCollisionShape(mc.level, front).isEmpty()
                && head.getCollisionShape(mc.level, front.above()).isEmpty();
    }

    private JsonObject processCommand(JsonObject cmd, ClientHandler client) {
        String action = cmd.has("action") ? cmd.get("action").getAsString() : "";
        JsonObject response = new JsonObject();

        long actionId = actionIdCounter.incrementAndGet();
        response.addProperty("action_id", actionId);
        response.addProperty("timestamp", System.currentTimeMillis());

        Minecraft mc = Minecraft.getInstance();

        if (action.equals("ping")) {
            response.addProperty("status", "pong");
            response.addProperty("server_time", System.currentTimeMillis());
            echoRequestId(cmd, response);
            return response;
        }

        if (action.equals("get_version")) {
            response = getVersion(actionId);
            echoRequestId(cmd, response);
            return response;
        }

        if (action.equals("get_metrics")) {
            response = getMetricsResponse(actionId);
            echoRequestId(cmd, response);
            return response;
        }

        if (action.equals("get_config")) {
            response = getConfigResponse(actionId);
            echoRequestId(cmd, response);
            return response;
        }

        if (action.equals("get_server_uptime")) {
            response = getServerUptime(actionId);
            echoRequestId(cmd, response);
            return response;
        }

        if (action.equals("open_singleplayer_world")) {
            response = openSingleplayerWorld(mc, cmd, actionId);
            echoRequestId(cmd, response);
            return response;
        }

        if (mc.player == null || mc.level == null) {
            JsonObject error = createError("PLAYER_NOT_READY", "Player not available", actionId);
            echoRequestId(cmd, error);
            return error;
        }

        try {
            commandsProcessed.incrementAndGet();

            switch (action) {
                case "get_full_state" -> response = getFullState(mc, actionId);
                case "get_player_state" -> response = getPlayerState(mc, actionId);
                case "get_inventory" -> response = getInventory(mc, actionId);
                case "get_environment" -> response = getEnvironment(mc, actionId);
                case "get_nearby_entities" -> response = getNearbyEntities(mc, cmd, actionId);
                case "get_nearby_blocks" -> response = getNearbyBlocks(mc, cmd, actionId);
                case "get_metrics" -> response = getMetricsResponse(actionId);
                case "get_snapshot" -> response = getSnapshot(mc, actionId);
                case "get_version" -> response = getVersion(actionId);
                case "get_config" -> response = getConfigResponse(actionId);
                case "get_vision" -> response = getVision(mc, cmd, actionId);
                case "get_visual_summary" -> response = getVisualSummary(mc, cmd, actionId);
                case "get_threats" -> response = getThreats(mc, cmd, actionId);
                case "get_survival" -> response = getSurvival(mc, actionId);
                case "get_combat" -> response = getCombat(mc, actionId);
                case "respawn" -> response = respawnPlayer(mc, actionId);
                case "set_game_mode" -> response = setGameMode(mc, cmd, actionId);
                case "set_difficulty" -> response = setDifficulty(mc, cmd, actionId);
                case "set_time" -> response = setTime(mc, cmd, actionId);

                case "move" -> response = executeMove(mc, cmd, actionId);
                case "look" -> response = executeLook(mc, cmd, actionId);
                case "jump" -> response = executeJump(mc, actionId);
                case "sneak" -> response = executeSneak(mc, cmd, actionId);
                case "sprint" -> response = executeSprint(mc, cmd, actionId);
                case "swim" -> response = executeSwim(mc, actionId);
                case "stop_moving" -> response = executeStopMoving(mc, actionId);
                case "crouch" -> response = executeCrouch(mc, cmd, actionId);
                case "turn" -> response = executeTurn(mc, cmd, actionId);

                case "attack" -> response = executeAttack(mc, cmd, actionId);
                case "use" -> response = executeUse(mc, cmd, actionId);
                case "block" -> response = executeBlock(mc, cmd, actionId);
                case "target_entity" -> response = targetEntity(mc, cmd, actionId);
                case "get_target" -> response = getTarget(mc, actionId);
                case "flee" -> response = executeFlee(mc, cmd, actionId);
                case "auto_attack" -> response = executeAutoAttack(mc, cmd, actionId);
                case "stop_attack" -> response = executeStopAttack(mc, actionId);
                case "aim_at_entity" -> response = aimAtEntity(mc, cmd, actionId);

                case "mine" -> response = executeMine(mc, cmd, actionId);
                case "place" -> response = executePlace(mc, cmd, actionId);
                case "get_block_info" -> response = getBlockInfo(mc, cmd, actionId);
                case "check_tool_validity" -> response = checkToolValidity(mc, cmd, actionId);
                case "get_mining_progress" -> response = getMiningProgress(cmd, actionId);
                case "stop_mining" -> response = stopMining(cmd, actionId);

                case "select_slot" -> response = executeSelectSlot(mc, cmd, actionId);
                case "swap_slots" -> response = executeSwapSlots(mc, cmd, actionId);
                case "drop_item" -> response = executeDropItem(mc, cmd, actionId);
                case "equip_armor" -> response = executeEquipArmor(mc, cmd, actionId);
                case "find_item" -> response = findItem(mc, cmd, actionId);
                case "sort_inventory" -> response = sortInventory(mc, actionId);
                case "stack_items" -> response = stackItems(mc, actionId);
                case "eat_food" -> response = eatFood(mc, cmd, actionId);
                case "select_best_tool" -> response = selectBestTool(mc, cmd, actionId);
                case "list_recipes" -> response = listRecipes(mc, cmd, actionId);
                case "query_recipe" -> response = queryRecipe(mc, cmd, actionId);
                case "craft" -> response = craftItem(mc, cmd, actionId);
                case "verify_crafting_recipes" -> response = verifyCraftingRecipes(mc, cmd, actionId);
                case "open_crafting_table" -> response = openCraftingTable(mc, cmd, actionId);
                case "get_container" -> response = getContainerState(mc, actionId);
                case "click_container_slot" -> response = clickContainerSlot(mc, cmd, actionId);
                case "close_container" -> response = closeContainer(mc, actionId);

                case "get_advancements" -> response = getAdvancements(mc, cmd, actionId);
                case "get_advancement_plan" -> response = getAdvancementPlan(mc, actionId);

                case "interact_entity" -> response = interactEntity(mc, cmd, actionId);
                case "ride_entity" -> response = rideEntity(mc, cmd, actionId);
                case "dismount" -> response = dismount(mc, actionId);
                case "trade_with_villager" -> response = tradeWithVillager(mc, cmd, actionId);
                case "feed_entity" -> response = feedEntity(mc, cmd, actionId);
                case "tame_entity" -> response = tameEntity(mc, cmd, actionId);

                case "scan_blocks" -> response = scanBlocks(mc, cmd, actionId);
                case "find_nearest_block" -> response = findNearestBlock(mc, cmd, actionId);
                case "get_biome" -> response = getBiome(mc, cmd, actionId);
                case "get_light_level" -> response = getLightLevel(mc, cmd, actionId);
                case "raycast" -> response = performRaycast(mc, cmd, actionId);

                case "mark_location" -> response = markLocation(mc, cmd, actionId);
                case "get_waypoints" -> response = getWaypoints(actionId);
                case "delete_waypoint" -> response = deleteWaypoint(cmd, actionId);
                case "navigate_to" -> response = navigateTo(mc, cmd, actionId);

                case "harvest" -> response = executeHarvest(mc, cmd, actionId);
                case "get_ai_status" -> response = getAIStatus(actionId);
                case "pause_harvest" -> response = pauseHarvest(cmd, actionId);
                case "resume_harvest" -> response = resumeHarvest(actionId);
                case "stop_harvest" -> response = stopHarvest(actionId);
                case "set_unpause" -> response = setUnpause(cmd, actionId);

                case "get_server_uptime" -> response = getServerUptime(actionId);
                case "open_singleplayer_world" -> response = openSingleplayerWorld(mc, cmd, actionId);
                case "stop_server" -> response = stopServerCommand(actionId);

                default -> response = createError("UNKNOWN_ACTION", "Unknown action: " + action, actionId);
            }

            actionHistory.put(actionId, new ActionRecord(action, System.currentTimeMillis()));

            if (actionHistory.size() > ACTION_HISTORY_SIZE) {
                List<Long> oldestKeys = actionHistory.keySet().stream()
                        .sorted()
                        .limit(ACTION_HISTORY_SIZE / 2)
                        .collect(Collectors.toList());
                oldestKeys.forEach(actionHistory::remove);
            }

        } catch (Throwable e) {
            rethrowIfCritical(e);
            LOGGER.error("Error processing action {}: {}", action, e.getMessage(), e);
            JsonObject error = createError("INTERNAL_ERROR", describeThrowable(e), actionId);
            echoRequestId(cmd, error);
            return error;
        }

        echoRequestId(cmd, response);
        return response;
    }

    private JsonObject openSingleplayerWorld(Minecraft mc, JsonObject cmd, long actionId) {
        String worldName = getString(cmd, "world", getString(cmd, "name", getString(cmd, "level", ""))).trim();
        if (worldName.isEmpty()) {
            return createError("MISSING_WORLD", "world/name/level must identify a local singleplayer save", actionId);
        }

        int minClientTicks = clampInt(getInt(cmd, "min_client_ticks", 80), 0, 1200);
        int maxClientTicks = clampInt(getInt(cmd, "max_client_ticks", 1200), Math.max(1, minClientTicks), 7200);
        boolean worldActive = mc.player != null && mc.level != null;
        if (worldActive) {
            MinecraftServer activeServer = mc.getSingleplayerServer();
            String activeWorld = activeServer == null ? "unknown" : activeServer.getWorldData().getLevelName();
            if (worldName.equals(activeWorld)) {
                JsonObject response = createSuccess(actionId);
                response.addProperty("world", worldName);
                response.addProperty("active_world", activeWorld);
                response.addProperty("queued", false);
                response.addProperty("already_in_world", true);
                response.addProperty("switch_requested", false);
                return response;
            }
            JsonObject error = createError(
                    "WORLD_ALREADY_ACTIVE",
                    "A different world is already active; restart or leave the world before opening '" + worldName + "'",
                    actionId
            );
            error.addProperty("world", worldName);
            error.addProperty("active_world", activeWorld);
            return error;
        }

        scheduleWorldOpen(new DelayedWorldOpen(worldName, minClientTicks, maxClientTicks), 0L);

        JsonObject response = createSuccess(actionId);
        response.addProperty("world", worldName);
        response.addProperty("queued", true);
        response.addProperty("min_client_ticks", minClientTicks);
        response.addProperty("max_client_ticks", maxClientTicks);
        response.addProperty("already_in_world", false);
        response.addProperty("switch_requested", false);
        return response;
    }

    private void scheduleWorldOpen(DelayedWorldOpen opener, long delayMs) {
        try {
            broadcaster.schedule(
                    () -> Minecraft.getInstance().execute(opener),
                    Math.max(0L, delayMs),
                    TimeUnit.MILLISECONDS
            );
        } catch (RejectedExecutionException e) {
            LOGGER.warn("Could not schedule singleplayer world open for '{}': {}",
                    opener.worldName, e.getMessage());
        }
    }

    private void echoRequestId(JsonObject cmd, JsonObject response) {
        if (cmd.has("request_id")) {
            response.add("request_id", cmd.get("request_id"));
        }
    }

    private String describeThrowable(Throwable error) {
        String message = error.getMessage();
        return error.getClass().getSimpleName()
                + ((message == null || message.isBlank()) ? "" : ": " + message);
    }

    private void rethrowIfCritical(Throwable error) {
        if (error instanceof VirtualMachineError vmError) {
            throw vmError;
        }
        if ("java.lang.ThreadDeath".equals(error.getClass().getName())) {
            BridgeServer.<RuntimeException>throwUnchecked(error);
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwUnchecked(Throwable error) throws E {
        throw (E) error;
    }

    private JsonObject createError(String code, String message, long actionId) {
        JsonObject error = new JsonObject();
        error.addProperty("status", "error");
        error.addProperty("code", code);
        error.addProperty("message", message);
        error.addProperty("action_id", actionId);
        error.addProperty("timestamp", System.currentTimeMillis());
        return error;
    }

    private JsonObject createSuccess(long actionId) {
        JsonObject success = new JsonObject();
        success.addProperty("status", "success");
        success.addProperty("action_id", actionId);
        success.addProperty("timestamp", System.currentTimeMillis());
        return success;
    }

    private boolean validateCoordinates(Minecraft mc, BlockPos pos) {
        if (mc.level == null) return false;
        if (pos.getY() < mc.level.getMinBuildHeight()) return false;
        if (pos.getY() > mc.level.getMaxBuildHeight()) return false;

        double dist = mc.player.getEyePosition().distanceTo(Vec3.atCenterOf(pos));
        return dist <= config.maxReachDistance;
    }

    private JsonObject getFullState(Minecraft mc, long actionId) {
        JsonObject state = createSuccess(actionId);
        state.add("player", buildPlayerState(mc));
        state.add("inventory", buildInventoryState(mc));
        state.add("environment", buildEnvironmentState(mc));
        state.add("nearby_entities", buildNearbyEntities(mc));
        state.add("nearby_blocks", buildNearbyBlocks(mc));
        state.add("vision", buildVisionState(mc));
        state.add("threats", buildThreatAssessment(mc));
        state.add("survival", buildSurvivalState(mc));
        state.add("combat", buildCombatState(mc));
        return state;
    }

    private JsonObject getPlayerState(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("player", buildPlayerState(mc));
        return response;
    }

    private JsonObject getInventory(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("inventory", buildInventoryState(mc));
        return response;
    }

    private JsonObject getEnvironment(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("environment", buildEnvironmentState(mc));
        return response;
    }

    private JsonObject getNearbyEntities(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("entities", buildNearbyEntities(mc));
        return response;
    }

    private JsonObject getNearbyBlocks(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("blocks", buildNearbyBlocks(mc));
        return response;
    }

    private JsonObject getMetricsResponse(long actionId) {
        JsonObject response = createSuccess(actionId);
        getMetrics().forEach(response::addProperty);
        return response;
    }

    private JsonObject getSnapshot(Minecraft mc, long actionId) {
        return getFullState(mc, actionId);
    }

    private String loadedModVersion(String modId) {
        return ModList.get()
                .getModContainerById(modId)
                .map(container -> container.getModInfo().getVersion().toString())
                .orElse("unknown");
    }

    private JsonObject getVersion(long actionId) {
        JsonObject response = createSuccess(actionId);
        response.addProperty("version", BRIDGE_VERSION);
        response.addProperty("api_level", BRIDGE_API_LEVEL);
        response.addProperty("minecraft_version", SharedConstants.getCurrentVersion().getName());
        response.addProperty("forge_version", loadedModVersion("forge"));
        response.addProperty("mod_id", ExampleMod.MODID);
        response.addProperty("mod_version", loadedModVersion(ExampleMod.MODID));
        return response;
    }

    private JsonObject getConfigResponse(long actionId) {
        JsonObject response = createSuccess(actionId);
        response.addProperty("max_actions_per_tick", config.maxActionsPerTick);
        response.addProperty("broadcast_interval_ms", config.broadcastIntervalMs);
        response.addProperty("nearby_entity_radius", config.nearbyEntityRadius);
        response.addProperty("nearby_block_radius", config.nearbyBlockRadius);
        response.addProperty("max_reach_distance", config.maxReachDistance);
        response.addProperty("max_raycast_distance", config.maxRaycastDistance);
        response.addProperty("combat_reach_distance", config.combatReachDistance);
        response.addProperty("threat_assessment_radius", config.threatAssessmentRadius);
        response.addProperty("auto_attack_radius", config.autoAttackRadius);
        response.addProperty("protected_zones_enabled", config.protectedZonesEnabled);
        return response;
    }

    private JsonObject getVision(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("vision", buildVisionState(mc, cmd));
        return response;
    }

    private JsonObject getThreats(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("threats", buildThreatAssessment(mc, cmd));
        return response;
    }

    private JsonObject getSurvival(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("survival", buildSurvivalState(mc));
        return response;
    }

    private JsonObject getCombat(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("combat", buildCombatState(mc));
        return response;
    }

    private JsonObject respawnPlayer(Minecraft mc, long actionId) {
        boolean wasDead = mc.player.isDeadOrDying() || mc.player.getHealth() <= 0.0f;
        actionQueue.offer(() -> {
            if (mc.player != null && (mc.player.isDeadOrDying() || mc.player.getHealth() <= 0.0f)) {
                controlState.clearMovement();
                navigationState = null;
                mc.player.respawn();
            }
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("queued", wasDead);
        response.addProperty("was_dead", wasDead);
        return response;
    }

    private JsonObject setGameMode(Minecraft mc, JsonObject cmd, long actionId) {
        String requestedMode = getString(cmd, "mode", "survival").toLowerCase(Locale.ROOT);
        GameType mode = GameType.byName(requestedMode, null);
        if (mode == null) {
            return createError("INVALID_GAME_MODE", "Mode must be survival, creative, adventure, or spectator", actionId);
        }

        MinecraftServer server = mc.getSingleplayerServer();
        if (server == null) {
            return createError("SERVER_UNAVAILABLE", "Game mode can only be changed in an integrated singleplayer server", actionId);
        }

        actionQueue.offer(() -> {
            ServerPlayer serverPlayer = server.getPlayerList().getPlayer(mc.player.getUUID());
            if (serverPlayer != null) {
                serverPlayer.setGameMode(mode);
            }
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("mode", mode.getName());
        response.addProperty("queued", true);
        return response;
    }

    private JsonObject setDifficulty(Minecraft mc, JsonObject cmd, long actionId) {
        String requested = getString(cmd, "difficulty", getString(cmd, "mode", "normal")).toLowerCase(Locale.ROOT);
        Difficulty difficulty = switch (requested) {
            case "peaceful", "0" -> Difficulty.PEACEFUL;
            case "easy", "1" -> Difficulty.EASY;
            case "normal", "2" -> Difficulty.NORMAL;
            case "hard", "3" -> Difficulty.HARD;
            default -> null;
        };
        if (difficulty == null) {
            return createError("INVALID_DIFFICULTY", "Difficulty must be peaceful, easy, normal, or hard", actionId);
        }

        MinecraftServer server = mc.getSingleplayerServer();
        if (server == null) {
            return createError("SERVER_UNAVAILABLE", "Difficulty can only be changed in an integrated singleplayer server", actionId);
        }

        actionQueue.offer(() -> server.setDifficulty(difficulty, true));

        JsonObject response = createSuccess(actionId);
        response.addProperty("difficulty", difficulty.getKey());
        response.addProperty("queued", true);
        return response;
    }

    private JsonObject setTime(Minecraft mc, JsonObject cmd, long actionId) {
        String namedTime = getString(cmd, "time", "");
        long time;
        if (!namedTime.isBlank()) {
            time = switch (namedTime.toLowerCase(Locale.ROOT)) {
                case "day" -> 1000L;
                case "noon" -> 6000L;
                case "night" -> 13000L;
                case "midnight" -> 18000L;
                default -> {
                    try {
                        yield Long.parseLong(namedTime);
                    } catch (NumberFormatException e) {
                        yield -1L;
                    }
                }
            };
        } else {
            time = getInt(cmd, "ticks", 1000);
        }
        if (time < 0L) {
            return createError("INVALID_TIME", "Time must be day, noon, night, midnight, or a non-negative tick value", actionId);
        }

        MinecraftServer server = mc.getSingleplayerServer();
        if (server == null) {
            return createError("SERVER_UNAVAILABLE", "Time can only be changed in an integrated singleplayer server", actionId);
        }

        long finalTime = time;
        actionQueue.offer(() -> server.getAllLevels().forEach(level -> level.setDayTime(finalTime)));

        JsonObject response = createSuccess(actionId);
        response.addProperty("time", time);
        response.addProperty("queued", true);
        return response;
    }

    private JsonObject getVisualSummary(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        JsonObject summary = new JsonObject();

        JsonObject wideCmd = new JsonObject();
        wideCmd.addProperty("rays", getInt(cmd, "rays", 17));
        wideCmd.addProperty("distance", getDouble(cmd, "distance", 72.0));
        wideCmd.addProperty("fov", getDouble(cmd, "fov", 110.0));
        JsonObject vision = buildVisionState(mc, wideCmd);
        JsonObject threats = buildThreatAssessment(mc, cmd);
        JsonObject survival = buildSurvivalState(mc);

        JsonArray rays = vision.getAsJsonArray("scan_rays");
        JsonArray resources = new JsonArray();
        JsonArray hazards = new JsonArray();
        JsonArray openings = new JsonArray();
        int misses = 0;
        int blockHits = 0;
        int entityHits = 0;

        for (JsonElement element : rays) {
            JsonObject ray = element.getAsJsonObject();
            String hitType = getString(ray, "hit_type", "miss");
            if ("miss".equals(hitType)) {
                misses++;
                openings.add(ray);
            } else if ("entity".equals(hitType)) {
                entityHits++;
            } else {
                blockHits++;
                if (ray.has("resource") && ray.get("resource").getAsBoolean()) {
                    resources.add(ray);
                }
                if (ray.has("hazard") && ray.get("hazard").getAsBoolean()) {
                    hazards.add(ray);
                }
            }
        }

        summary.add("vision", vision);
        summary.add("threats", threats);
        summary.add("survival", survival);
        summary.add("visible_resources", resources);
        summary.add("visible_hazards", hazards);
        summary.add("openings", openings);
        summary.addProperty("resource_count", resources.size());
        summary.addProperty("hazard_count", hazards.size());
        summary.addProperty("opening_count", openings.size());
        summary.addProperty("miss_ray_count", misses);
        summary.addProperty("block_ray_count", blockHits);
        summary.addProperty("entity_ray_count", entityHits);
        summary.addProperty("recommended_focus", visualFocusRecommendation(summary));

        response.add("visual_summary", summary);
        return response;
    }

    private String visualFocusRecommendation(JsonObject summary) {
        JsonObject survival = summary.getAsJsonObject("survival");
        if (survival != null) {
            if (survival.has("immediate_danger") && survival.get("immediate_danger").getAsBoolean()) {
                return "escape_hazard";
            }
            if (survival.has("should_flee") && survival.get("should_flee").getAsBoolean()) {
                return "track_threats";
            }
            if (survival.has("should_eat") && survival.get("should_eat").getAsBoolean()) {
                return "eat_or_find_food";
            }
        }
        if (summary.get("visible_resources").getAsJsonArray().size() > 0) {
            return "collect_visible_resource";
        }
        if (summary.get("openings").getAsJsonArray().size() > 0) {
            return "move_through_opening";
        }
        return "turn_and_scan";
    }

    private JsonObject listRecipes(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        JsonArray recipes = new JsonArray();

        boolean includeVanilla = !cmd.has("include_vanilla") || cmd.get("include_vanilla").getAsBoolean();
        boolean includeDetails = cmd.has("include_details") && cmd.get("include_details").getAsBoolean();
        boolean includePlan = includeDetails || (cmd.has("include_plan") && cmd.get("include_plan").getAsBoolean());
        boolean craftableOnly = cmd.has("craftable_only") && cmd.get("craftable_only").getAsBoolean();
        int vanillaLimit = Math.max(0, Math.min(2000, getInt(cmd, "vanilla_limit", getInt(cmd, "limit", 120))));
        int maxCrafts = Math.max(1, Math.min(64, getInt(cmd, "max_crafts", 1)));
        int vanillaCrafting = 0;
        int genericSupported = 0;
        int currentCraftable = 0;
        int unsupportedTemplates = 0;

        if (includeVanilla && mc.level != null) {
            List<RecipeHolder<CraftingRecipe>> holders = mc.level.getRecipeManager().getAllRecipesFor(RecipeType.CRAFTING).stream()
                    .filter(holder -> "minecraft".equals(holder.id().getNamespace()))
                    .sorted(Comparator.comparing(holder -> holder.id().toString()))
                    .toList();
            for (RecipeHolder<CraftingRecipe> holder : holders) {
                vanillaCrafting++;
                String templateFailure = validateRecipeTemplate(holder, 3, 3);
                boolean templateSupported = templateFailure == null;
                if (templateSupported) {
                    genericSupported++;
                } else {
                    unsupportedTemplates++;
                }

                CraftingPlanResult planResult = templateSupported
                        ? buildCraftingExecutionPlan(mc, holder, maxCrafts)
                        : new CraftingPlanResult(null, "UNSUPPORTED_RECIPE_KIND",
                        templateFailure, holder, new JsonArray());
                boolean craftable = planResult.plan() != null && planResult.plan().crafts() > 0;
                if (craftable) {
                    currentCraftable++;
                }
                if (craftableOnly && !craftable) {
                    continue;
                }
                if (recipes.size() >= vanillaLimit) {
                    continue;
                }
                if (includeDetails) {
                    recipes.add(describeCraftingRecipeListEntry(mc, holder, planResult, templateFailure, includePlan));
                } else {
                    recipes.add(holder.id().toString());
                }
            }
        }

        response.add("crafting_context", describeCraftingContext(mc));
        response.add("supported_recipes", recipes);
        response.add(includeDetails ? "vanilla_recipes" : "vanilla_recipe_ids", recipes.deepCopy());
        response.addProperty("vanilla_crafting_recipes", vanillaCrafting);
        response.addProperty("generic_supported_count", genericSupported);
        response.addProperty("unsupported_template_count", unsupportedTemplates);
        response.addProperty("current_inventory_craftable", currentCraftable);
        response.addProperty("returned_count", recipes.size());
        response.addProperty("supported_count", genericSupported);
        response.addProperty("generic_crafting_engine", "vanilla_container_clicks");
        return response;
    }

    private JsonObject queryRecipe(Minecraft mc, JsonObject cmd, long actionId) {
        String target = getString(cmd, "item", getString(cmd, "recipe", ""));
        if (target.isBlank()) {
            return createError("MISSING_PARAM", "item or recipe required", actionId);
        }

        JsonObject response = createSuccess(actionId);

        List<RecipeHolder<CraftingRecipe>> candidates = findCraftingRecipes(mc, target);
        if (!candidates.isEmpty()) {
            CraftingPlanResult planResult = buildCraftingExecutionPlanForTarget(mc, target, 1);
            RecipeHolder<CraftingRecipe> recipe = planResult.recipe() != null ? planResult.recipe() : candidates.get(0);
            response.addProperty("found", true);
            response.addProperty("supported", isSupportedCraftingRecipe(recipe.value()));
            response.addProperty("craft_bridge_support", isSupportedCraftingRecipe(recipe.value())
                    ? "generic_container_clicks"
                    : "unsupported_recipe_kind");
            response.addProperty("candidate_count", candidates.size());
            response.add("crafting_context", describeCraftingContext(mc));
            response.add("recipe", describeCraftingRecipe(mc, recipe));
            if (planResult.plan() != null) {
                response.addProperty("craftable", planResult.plan().crafts() > 0);
                response.add("plan", describeCraftingPlan(planResult.plan()));
            } else {
                response.addProperty("craftable", false);
                response.addProperty("reason", planResult.code());
                response.addProperty("message", planResult.message());
                response.add("missing", planResult.missing());
            }
            return response;
        }

        RecipeHolder<?> vanilla = findVanillaRecipe(mc, target);
        if (vanilla == null) {
            response.addProperty("supported", false);
            response.addProperty("found", false);
            return response;
        }

        response.addProperty("found", true);
        response.addProperty("supported", false);
        response.addProperty("craft_bridge_support", "query_only_non_crafting_recipe");
        response.add("recipe", describeVanillaRecipe(mc, vanilla));
        return response;
    }

    private JsonObject craftItem(Minecraft mc, JsonObject cmd, long actionId) {
        String target = getString(cmd, "item", getString(cmd, "recipe", ""));
        if (target.isBlank()) {
            return createError("MISSING_PARAM", "item or recipe required", actionId);
        }

        int maxCrafts = Math.max(1, Math.min(64, getInt(cmd, "max_crafts", 1)));
        CraftingPlanResult planResult = buildCraftingExecutionPlanForTarget(mc, target, maxCrafts);
        CraftingExecutionPlan plan = planResult.plan();

        if (plan == null) {
            if ("MISSING_INGREDIENTS".equals(planResult.code())
                    || "CRAFTING_TABLE_REQUIRED".equals(planResult.code())
                    || "NO_CRAFTING_GRID".equals(planResult.code())
                    || "CURSOR_NOT_EMPTY".equals(planResult.code())) {
                JsonObject response = createSuccess(actionId);
                response.addProperty("crafted", false);
                response.addProperty("reason", planResult.code().toLowerCase(Locale.ROOT));
                response.addProperty("message", planResult.message());
                response.add("missing", planResult.missing());
                if (planResult.recipe() != null) {
                    response.add("recipe", describeCraftingRecipe(mc, planResult.recipe()));
                }
                response.add("crafting_context", describeCraftingContext(mc));
                return response;
            }

            JsonObject response = createError("UNSUPPORTED_RECIPE",
                    planResult.message(),
                    actionId);
            RecipeHolder<?> vanilla = findVanillaRecipe(mc, target);
            if (vanilla != null) {
                response.add("recipe", describeVanillaRecipe(mc, vanilla));
            }
            return response;
        }

        if (plan.crafts() <= 0) {
            JsonObject response = createSuccess(actionId);
            response.addProperty("crafted", false);
            response.addProperty("recipe", plan.recipeId());
            response.addProperty("reason", "missing_ingredients");
            response.add("missing", plan.missing());
            response.add("ingredients", plan.ingredients());
            response.addProperty("item", itemId(plan.result().getItem()));
            return response;
        }

        actionQueue.offer(() -> executeCraftingPlan(mc, plan));

        JsonObject response = createSuccess(actionId);
        response.addProperty("crafted", true);
        response.addProperty("recipe", plan.recipeId());
        response.addProperty("item", itemId(plan.result().getItem()));
        response.addProperty("crafts", plan.crafts());
        response.addProperty("output_count", plan.outputCount());
        response.addProperty("requires_crafting_table", plan.requiresCraftingTable());
        response.addProperty("using_crafting_table", plan.usingCraftingTable());
        response.addProperty("container_id", plan.containerId());
        response.addProperty("craft_engine", "vanilla_container_clicks");
        response.add("ingredients", plan.ingredients());
        response.add("placements", plan.placementsJson());
        response.add("recipe_detail", describeCraftingRecipe(mc, planResult.recipe()));
        response.add("inventory_before", buildInventoryState(mc));
        return response;
    }

    private JsonObject verifyCraftingRecipes(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        JsonArray unsupported = new JsonArray();
        JsonArray currentFailures = new JsonArray();
        boolean includeRecipes = cmd.has("include_recipes") && cmd.get("include_recipes").getAsBoolean();
        int detailLimit = Math.max(0, Math.min(500, getInt(cmd, "detail_limit", 80)));

        int vanillaCrafting = 0;
        int shaped = 0;
        int shapeless = 0;
        int templateSupported = 0;
        int specialOrDynamic = 0;
        int currentCraftable = 0;
        int currentPlanFailures = 0;
        JsonArray supportedRecipes = new JsonArray();

        if (mc.level != null) {
            for (RecipeHolder<CraftingRecipe> holder : mc.level.getRecipeManager().getAllRecipesFor(RecipeType.CRAFTING)) {
                if (!"minecraft".equals(holder.id().getNamespace())) {
                    continue;
                }
                vanillaCrafting++;
                CraftingRecipe recipe = holder.value();
                boolean shapedRecipe = recipe instanceof ShapedRecipe;
                boolean shapelessRecipe = recipe instanceof ShapelessRecipe;
                if (shapedRecipe) {
                    shaped++;
                }
                if (shapelessRecipe) {
                    shapeless++;
                }

                String templateFailure = validateRecipeTemplate(holder, 3, 3);
                if (templateFailure == null) {
                    templateSupported++;
                    if (includeRecipes && supportedRecipes.size() < detailLimit) {
                        supportedRecipes.add(describeCraftingRecipe(mc, holder));
                    }
                } else {
                    if (!shapedRecipe && !shapelessRecipe) {
                        specialOrDynamic++;
                    }
                    JsonObject failure = new JsonObject();
                    failure.addProperty("id", holder.id().toString());
                    failure.addProperty("reason", templateFailure);
                    failure.addProperty("serializer", String.valueOf(recipe.getSerializer()));
                    unsupported.add(failure);
                }

                CraftingPlanResult planResult = buildCraftingExecutionPlan(mc, holder, 1);
                if (planResult.plan() != null && planResult.plan().crafts() > 0) {
                    currentCraftable++;
                } else if (planResult.plan() == null
                        && !"MISSING_INGREDIENTS".equals(planResult.code())
                        && !"CRAFTING_TABLE_REQUIRED".equals(planResult.code())
                        && !"NO_CRAFTING_GRID".equals(planResult.code())) {
                    currentPlanFailures++;
                    if (currentFailures.size() < detailLimit) {
                        JsonObject failure = new JsonObject();
                        failure.addProperty("id", holder.id().toString());
                        failure.addProperty("reason", planResult.code());
                        failure.addProperty("message", planResult.message());
                        currentFailures.add(failure);
                    }
                }
            }
        }

        response.add("crafting_context", describeCraftingContext(mc));
        response.addProperty("vanilla_crafting_recipes", vanillaCrafting);
        response.addProperty("vanilla_shaped_recipes", shaped);
        response.addProperty("vanilla_shapeless_recipes", shapeless);
        response.addProperty("template_supported", templateSupported);
        response.addProperty("special_or_dynamic_skipped", specialOrDynamic);
        response.addProperty("unsupported_template_count", unsupported.size());
        response.addProperty("current_inventory_craftable", currentCraftable);
        response.addProperty("current_plan_failures", currentPlanFailures);
        response.addProperty("all_vanilla_shaped_shapeless_workbench_templates_verified", unsupported.size() == specialOrDynamic);
        response.add("unsupported_templates", unsupported);
        response.add("current_plan_failures_detail", currentFailures);
        if (includeRecipes) {
            response.add("supported_recipe_details", supportedRecipes);
        }
        return response;
    }

    private JsonObject openCraftingTable(Minecraft mc, JsonObject cmd, long actionId) {
        BlockPos pos;
        if (cmd.has("x") && cmd.has("y") && cmd.has("z")) {
            pos = new BlockPos(cmd.get("x").getAsInt(), cmd.get("y").getAsInt(), cmd.get("z").getAsInt());
        } else {
            pos = findNearestBlockById(mc, "minecraft:crafting_table", getInt(cmd, "radius", 8));
        }

        if (pos == null) {
            return createError("CRAFTING_TABLE_NOT_FOUND", "No crafting table found nearby", actionId);
        }
        if (!"minecraft:crafting_table".equals(blockId(mc.level.getBlockState(pos)))) {
            return createError("NOT_CRAFTING_TABLE", "Target block is not a crafting table", actionId);
        }

        double distance = mc.player.position().distanceTo(Vec3.atCenterOf(pos));
        if (distance > config.maxReachDistance) {
            JsonObject response = createSuccess(actionId);
            response.addProperty("opened", false);
            response.addProperty("reason", "out_of_reach");
            response.addProperty("x", pos.getX());
            response.addProperty("y", pos.getY());
            response.addProperty("z", pos.getZ());
            response.addProperty("distance", distance);
            response.addProperty("recommended_action", "navigate_to");
            return response;
        }

        BlockPos tablePos = pos;
        actionQueue.offer(() -> {
            BlockHitResult hit = new BlockHitResult(Vec3.atCenterOf(tablePos), Direction.UP, tablePos, false);
            mc.gameMode.useItemOn(mc.player, InteractionHand.MAIN_HAND, hit);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("opened", true);
        response.addProperty("x", pos.getX());
        response.addProperty("y", pos.getY());
        response.addProperty("z", pos.getZ());
        response.addProperty("distance", distance);
        return response;
    }

    private JsonObject getContainerState(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("container", describeContainer(mc));
        return response;
    }

    private JsonObject clickContainerSlot(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("slot")) {
            return createError("MISSING_PARAM", "slot required", actionId);
        }
        if (mc.player.containerMenu == null) {
            return createError("NO_CONTAINER", "No active container menu", actionId);
        }

        int slot = cmd.get("slot").getAsInt();
        int button = getInt(cmd, "button", 0);
        ClickType clickType;
        try {
            clickType = ClickType.valueOf(getString(cmd, "click_type", "PICKUP").toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return createError("INVALID_CLICK_TYPE", "Unsupported click_type", actionId);
        }

        AbstractContainerMenu menu = mc.player.containerMenu;
        if (!menu.isValidSlotIndex(slot) && slot != AbstractContainerMenu.SLOT_CLICKED_OUTSIDE) {
            return createError("INVALID_SLOT", "Slot is not valid for current container", actionId);
        }

        actionQueue.offer(() -> mc.gameMode.handleInventoryMouseClick(
                menu.containerId, slot, button, clickType, mc.player
        ));

        JsonObject response = createSuccess(actionId);
        response.addProperty("container_id", menu.containerId);
        response.addProperty("slot", slot);
        response.addProperty("button", button);
        response.addProperty("click_type", clickType.name());
        return response;
    }

    private JsonObject closeContainer(Minecraft mc, long actionId) {
        actionQueue.offer(() -> mc.player.closeContainer());
        JsonObject response = createSuccess(actionId);
        response.addProperty("closed", true);
        return response;
    }

    private JsonObject getAdvancements(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        JsonArray completed = new JsonArray();
        JsonArray missing = new JsonArray();
        JsonArray progress = new JsonArray();
        Set<String> completedIds = new HashSet<>();
        int limit = Math.max(1, Math.min(1000, getInt(cmd, "limit", 500)));

        MinecraftServer server = mc.getSingleplayerServer();
        if (server == null) {
            response.addProperty("available", false);
            response.addProperty("reason", "advancement_progress_requires_integrated_singleplayer_server");
        } else {
            ServerPlayer serverPlayer = server.getPlayerList().getPlayer(mc.player.getUUID());
            if (serverPlayer == null) {
                response.addProperty("available", false);
                response.addProperty("reason", "server_player_not_found");
            } else {
                PlayerAdvancements playerAdvancements = serverPlayer.getAdvancements();
                ServerAdvancementManager manager = server.getAdvancements();
                int count = 0;
                for (AdvancementHolder holder : manager.getAllAdvancements()) {
                    if (count++ >= limit) {
                        break;
                    }
                    AdvancementProgress advancementProgress = playerAdvancements.getOrStartProgress(holder);
                    JsonObject obj = describeAdvancementProgress(holder, advancementProgress);
                    progress.add(obj);
                    if (advancementProgress.isDone()) {
                        completed.add(holder.id().toString());
                        completedIds.add(holder.id().toString());
                    } else if (!holder.id().getPath().startsWith("recipes/")) {
                        missing.add(holder.id().toString());
                    }
                }
                response.addProperty("available", true);
            }
        }

        JsonObject curriculum = buildAdvancementCurriculum(mc, completedIds);
        response.add("completed", completed);
        response.add("missing", missing);
        response.add("progress", progress);
        response.add("curriculum", curriculum);
        response.addProperty("completed_count", completed.size());
        response.addProperty("missing_count", missing.size());
        return response;
    }

    private JsonObject getAdvancementPlan(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);
        response.add("plan", buildAdvancementCurriculum(mc, readCompletedAdvancementIds(mc, 1000)));
        return response;
    }

    private RecipeHolder<?> findVanillaRecipe(Minecraft mc, String target) {
        if (mc.level == null) {
            return null;
        }
        String normalized = normalizeItemTarget(target);
        ResourceLocation directId = ResourceLocation.tryParse(target.contains(":") ? target : "minecraft:" + normalized);
        if (directId != null) {
            Optional<RecipeHolder<?>> direct = mc.level.getRecipeManager().byKey(directId);
            if (direct.isPresent()) {
                return direct.get();
            }
        }
        for (RecipeHolder<?> holder : mc.level.getRecipeManager().getRecipes()) {
            String id = holder.id().toString().toLowerCase(Locale.ROOT);
            if (id.contains(normalized)) {
                return holder;
            }
            ItemStack result = holder.value().getResultItem(mc.level.registryAccess());
            if (!result.isEmpty() && itemId(result.getItem()).contains(normalized)) {
                return holder;
            }
        }
        return null;
    }

    private JsonObject describeVanillaRecipe(Minecraft mc, RecipeHolder<?> holder) {
        JsonObject obj = new JsonObject();
        obj.addProperty("id", holder.id().toString());
        obj.addProperty("type", String.valueOf(holder.value().getType()));
        obj.addProperty("serializer", String.valueOf(holder.value().getSerializer()));

        ItemStack result = holder.value().getResultItem(mc.level.registryAccess());
        obj.addProperty("result", result.isEmpty() ? "empty" : itemId(result.getItem()));
        obj.addProperty("result_count", result.getCount());

        JsonArray ingredients = new JsonArray();
        for (Ingredient ingredient : holder.value().getIngredients()) {
            if (ingredient.isEmpty()) {
                continue;
            }
            JsonObject entry = new JsonObject();
            JsonArray options = new JsonArray();
            for (ItemStack option : ingredient.getItems()) {
                if (!option.isEmpty()) {
                    options.add(itemId(option.getItem()));
                }
            }
            entry.add("options", options);
            ingredients.add(entry);
        }
        obj.add("ingredients", ingredients);
        obj.addProperty("ingredient_slots", ingredients.size());
        return obj;
    }

    private JsonObject describeCraftingRecipe(Minecraft mc, RecipeHolder<CraftingRecipe> holder) {
        JsonObject obj = describeVanillaRecipe(mc, holder);
        CraftingRecipe recipe = holder.value();
        obj.addProperty("crafting_recipe", true);
        obj.addProperty("bridge_supported", isSupportedCraftingRecipe(recipe));
        obj.addProperty("requires_crafting_table", requiresCraftingTable(recipe));
        obj.addProperty("fits_player_grid", recipe.canCraftInDimensions(2, 2));
        obj.addProperty("fits_workbench", recipe.canCraftInDimensions(3, 3));

        if (recipe instanceof ShapedRecipe shapedRecipe) {
            obj.addProperty("shape", "shaped");
            obj.addProperty("width", shapedRecipe.getWidth());
            obj.addProperty("height", shapedRecipe.getHeight());
            obj.add("grid_template", recipeTemplateJson(shapedRecipe.getIngredients(), shapedRecipe.getWidth(), shapedRecipe.getHeight(), 3));
        } else if (recipe instanceof ShapelessRecipe) {
            obj.addProperty("shape", "shapeless");
            obj.addProperty("width", 0);
            obj.addProperty("height", 0);
            obj.add("grid_template", recipeTemplateJson(recipe.getIngredients(), 0, 0, 3));
        } else {
            obj.addProperty("shape", "dynamic_or_special");
        }
        return obj;
    }

    private JsonObject describeCraftingRecipeListEntry(
            Minecraft mc,
            RecipeHolder<CraftingRecipe> holder,
            CraftingPlanResult planResult,
            String templateFailure,
            boolean includePlan
    ) {
        JsonObject obj = describeCraftingRecipe(mc, holder);
        boolean templateSupported = templateFailure == null;
        obj.addProperty("template_supported", templateSupported);
        if (!templateSupported) {
            obj.addProperty("template_failure", templateFailure);
        }

        CraftingExecutionPlan plan = planResult.plan();
        boolean craftable = plan != null && plan.crafts() > 0;
        obj.addProperty("current_inventory_craftable", craftable);
        obj.addProperty("plan_status", planResult.code());
        obj.addProperty("plan_message", planResult.message());
        obj.add("missing", planResult.missing());
        if (includePlan && plan != null) {
            obj.add("plan", describeCraftingPlan(plan));
        }
        return obj;
    }

    private JsonArray recipeTemplateJson(List<Ingredient> ingredients, int recipeWidth, int recipeHeight, int gridWidth) {
        JsonArray template = new JsonArray();
        int placed = 0;
        for (int index = 0; index < ingredients.size(); index++) {
            Ingredient ingredient = ingredients.get(index);
            if (ingredient.isEmpty()) {
                continue;
            }
            int col;
            int row;
            if (recipeWidth > 0) {
                col = index % recipeWidth;
                row = index / recipeWidth;
            } else {
                col = placed % gridWidth;
                row = placed / gridWidth;
            }
            JsonObject slot = new JsonObject();
            slot.addProperty("ingredient_index", index);
            slot.addProperty("grid_col", col);
            slot.addProperty("grid_row", row);
            slot.add("options", ingredientOptionsJson(ingredient));
            template.add(slot);
            placed++;
        }
        return template;
    }

    private JsonArray ingredientOptionsJson(Ingredient ingredient) {
        JsonArray options = new JsonArray();
        for (ItemStack option : ingredient.getItems()) {
            if (!option.isEmpty()) {
                options.add(itemId(option.getItem()));
            }
        }
        return options;
    }

    private CraftingPlanResult buildCraftingExecutionPlanForTarget(Minecraft mc, String target, int maxCrafts) {
        List<RecipeHolder<CraftingRecipe>> candidates = findCraftingRecipes(mc, target);
        if (candidates.isEmpty()) {
            return new CraftingPlanResult(null, "UNSUPPORTED_RECIPE",
                    "No vanilla crafting recipe matched target: " + target, null, new JsonArray());
        }

        CraftingPlanResult bestMissing = null;
        CraftingPlanResult bestOther = null;
        for (RecipeHolder<CraftingRecipe> candidate : candidates) {
            CraftingPlanResult result = buildCraftingExecutionPlan(mc, candidate, maxCrafts);
            if (result.plan() != null && result.plan().crafts() > 0) {
                return result;
            }
            if ("MISSING_INGREDIENTS".equals(result.code())) {
                if (bestMissing == null || result.missing().size() < bestMissing.missing().size()) {
                    bestMissing = result;
                }
            } else if (bestOther == null) {
                bestOther = result;
            }
        }
        return bestMissing != null ? bestMissing : bestOther;
    }

    private CraftingPlanResult buildCraftingExecutionPlan(Minecraft mc, RecipeHolder<CraftingRecipe> holder, int maxCrafts) {
        CraftingGridContext context = craftingGridContext(mc);
        if (context == null) {
            return new CraftingPlanResult(null, "NO_CRAFTING_GRID",
                    "Open the player inventory or a crafting table before crafting.", holder, new JsonArray());
        }
        if (!mc.player.containerMenu.getCarried().isEmpty()) {
            return new CraftingPlanResult(null, "CURSOR_NOT_EMPTY",
                    "The container cursor is carrying an item; clear it before automated crafting.", holder, new JsonArray());
        }
        return buildCraftingExecutionPlan(mc, holder, context, maxCrafts);
    }

    private CraftingPlanResult buildCraftingExecutionPlan(
            Minecraft mc,
            RecipeHolder<CraftingRecipe> holder,
            CraftingGridContext context,
            int maxCrafts
    ) {
        CraftingRecipe recipe = holder.value();
        if (!isSupportedCraftingRecipe(recipe)) {
            return new CraftingPlanResult(null, "UNSUPPORTED_RECIPE_KIND",
                    "Only shaped and shapeless crafting recipes can be executed generically.", holder, new JsonArray());
        }
        if (!recipe.canCraftInDimensions(context.gridWidth(), context.gridHeight())) {
            String message = recipe.canCraftInDimensions(3, 3)
                    ? "Recipe requires a crafting table; call open_crafting_table first."
                    : "Recipe does not fit a vanilla 3x3 workbench.";
            return new CraftingPlanResult(null,
                    recipe.canCraftInDimensions(3, 3) ? "CRAFTING_TABLE_REQUIRED" : "RECIPE_TOO_LARGE",
                    message,
                    holder,
                    new JsonArray());
        }

        List<IngredientTarget> targets = ingredientTargetsForRecipe(recipe, context);
        if (targets.isEmpty()) {
            return new CraftingPlanResult(null, "UNSUPPORTED_RECIPE_KIND",
                    "Recipe has no concrete ingredient targets.", holder, new JsonArray());
        }

        AssignmentResult bestFailure = null;
        int cappedCrafts = Math.max(1, Math.min(64, maxCrafts));
        for (int crafts = cappedCrafts; crafts >= 1; crafts--) {
            AssignmentResult assignment = assignIngredientSources(mc, context, targets, crafts);
            if (assignment.success()) {
                ItemStack result = recipe.getResultItem(mc.level.registryAccess()).copy();
                int outputCount = result.getCount() * crafts;
                CraftingExecutionPlan plan = new CraftingExecutionPlan(
                        holder.id().toString(),
                        result,
                        outputCount,
                        crafts,
                        requiresCraftingTable(recipe),
                        context.usingCraftingTable(),
                        context.containerId(),
                        context.resultSlot(),
                        List.copyOf(context.gridSlots()),
                        List.copyOf(assignment.placements()),
                        assignment.ingredientsJson(),
                        assignment.missing(),
                        assignment.placementsJson()
                );
                return new CraftingPlanResult(plan, "OK", "Recipe can be executed.", holder, assignment.missing());
            }
            bestFailure = assignment;
        }

        JsonArray missing = bestFailure == null ? new JsonArray() : bestFailure.missing();
        CraftingExecutionPlan emptyPlan = new CraftingExecutionPlan(
                holder.id().toString(),
                recipe.getResultItem(mc.level.registryAccess()).copy(),
                0,
                0,
                requiresCraftingTable(recipe),
                context.usingCraftingTable(),
                context.containerId(),
                context.resultSlot(),
                List.copyOf(context.gridSlots()),
                List.of(),
                bestFailure == null ? new JsonArray() : bestFailure.ingredientsJson(),
                missing,
                new JsonArray()
        );
        return new CraftingPlanResult(emptyPlan, "MISSING_INGREDIENTS",
                "Inventory does not contain the ingredients for this recipe.", holder, missing);
    }

    private List<IngredientTarget> ingredientTargetsForRecipe(CraftingRecipe recipe, CraftingGridContext context) {
        List<IngredientTarget> targets = new ArrayList<>();
        if (recipe instanceof ShapedRecipe shapedRecipe) {
            int recipeWidth = shapedRecipe.getWidth();
            int recipeHeight = shapedRecipe.getHeight();
            if (recipeWidth > context.gridWidth() || recipeHeight > context.gridHeight()) {
                return targets;
            }
            List<Ingredient> ingredients = shapedRecipe.getIngredients();
            for (int row = 0; row < recipeHeight; row++) {
                for (int col = 0; col < recipeWidth; col++) {
                    int ingredientIndex = col + row * recipeWidth;
                    Ingredient ingredient = ingredients.get(ingredientIndex);
                    if (ingredient.isEmpty()) {
                        continue;
                    }
                    int gridSlot = context.gridSlot(col, row);
                    targets.add(new IngredientTarget(ingredientIndex, ingredient, gridSlot, col, row));
                }
            }
        } else if (recipe instanceof ShapelessRecipe) {
            int placed = 0;
            List<Ingredient> ingredients = recipe.getIngredients();
            for (int ingredientIndex = 0; ingredientIndex < ingredients.size(); ingredientIndex++) {
                Ingredient ingredient = ingredients.get(ingredientIndex);
                if (ingredient.isEmpty()) {
                    continue;
                }
                if (placed >= context.gridSlots().size()) {
                    return List.of();
                }
                int gridSlot = context.gridSlots().get(placed);
                int col = placed % context.gridWidth();
                int row = placed / context.gridWidth();
                targets.add(new IngredientTarget(ingredientIndex, ingredient, gridSlot, col, row));
                placed++;
            }
        }
        return targets;
    }

    private AssignmentResult assignIngredientSources(
            Minecraft mc,
            CraftingGridContext context,
            List<IngredientTarget> targets,
            int crafts
    ) {
        AbstractContainerMenu menu = mc.player.containerMenu;
        Map<Integer, Integer> remaining = new LinkedHashMap<>();
        Map<Integer, ItemStack> sourceStacks = new LinkedHashMap<>();
        Inventory inv = mc.player.getInventory();

        for (int menuSlot = 0; menuSlot < menu.slots.size(); menuSlot++) {
            Slot slot = menu.slots.get(menuSlot);
            ItemStack stack = slot.getItem();
            int inventorySlot = slot.getContainerSlot();
            if (slot.container == inv
                    && inventorySlot >= 0
                    && inventorySlot < 36
                    && !stack.isEmpty()
                    && slot.mayPickup(mc.player)) {
                remaining.put(menuSlot, stack.getCount());
                sourceStacks.put(menuSlot, stack.copy());
            }
        }

        List<IngredientPlacement> placements = new ArrayList<>();
        JsonArray ingredientsJson = new JsonArray();
        JsonArray missing = new JsonArray();

        for (IngredientTarget target : targets) {
            Integer selectedSlot = null;
            ItemStack selectedStack = ItemStack.EMPTY;
            for (Map.Entry<Integer, ItemStack> entry : sourceStacks.entrySet()) {
                int menuSlot = entry.getKey();
                ItemStack stack = entry.getValue();
                int available = remaining.getOrDefault(menuSlot, 0);
                if (available >= crafts && target.ingredient().test(stack)) {
                    selectedSlot = menuSlot;
                    selectedStack = stack;
                    break;
                }
            }

            JsonObject ingredientJson = new JsonObject();
            ingredientJson.addProperty("ingredient_index", target.ingredientIndex());
            ingredientJson.addProperty("grid_slot", target.gridSlot());
            ingredientJson.addProperty("grid_col", target.gridCol());
            ingredientJson.addProperty("grid_row", target.gridRow());
            ingredientJson.addProperty("count_per_craft", 1);
            ingredientJson.addProperty("count", crafts);
            ingredientJson.add("options", ingredientOptionsJson(target.ingredient()));

            if (selectedSlot == null) {
                JsonObject missingIngredient = new JsonObject();
                missingIngredient.addProperty("ingredient_index", target.ingredientIndex());
                missingIngredient.addProperty("count", crafts);
                missingIngredient.add("options", ingredientOptionsJson(target.ingredient()));
                missing.add(missingIngredient);
                ingredientJson.addProperty("available", 0);
                ingredientsJson.add(ingredientJson);
                continue;
            }

            remaining.put(selectedSlot, remaining.get(selectedSlot) - crafts);
            String selectedItem = itemId(selectedStack.getItem());
            Slot sourceSlot = menu.slots.get(selectedSlot);
            ingredientJson.addProperty("selected_item", selectedItem);
            ingredientJson.addProperty("source_slot", selectedSlot);
            ingredientJson.addProperty("source_container_slot", sourceSlot.getContainerSlot());
            ingredientJson.addProperty("available", remaining.get(selectedSlot) + crafts);
            ingredientsJson.add(ingredientJson);

            placements.add(new IngredientPlacement(
                    target.ingredientIndex(),
                    selectedSlot,
                    sourceSlot.getContainerSlot(),
                    target.gridSlot(),
                    target.gridCol(),
                    target.gridRow(),
                    selectedItem,
                    crafts
            ));
        }

        return new AssignmentResult(missing.size() == 0, placements, ingredientsJson, missing, placementsJson(placements));
    }

    private JsonArray placementsJson(List<IngredientPlacement> placements) {
        JsonArray array = new JsonArray();
        for (IngredientPlacement placement : placements) {
            JsonObject obj = new JsonObject();
            obj.addProperty("ingredient_index", placement.ingredientIndex());
            obj.addProperty("source_slot", placement.sourceSlot());
            obj.addProperty("source_container_slot", placement.sourceContainerSlot());
            obj.addProperty("grid_slot", placement.gridSlot());
            obj.addProperty("grid_col", placement.gridCol());
            obj.addProperty("grid_row", placement.gridRow());
            obj.addProperty("item", placement.itemId());
            obj.addProperty("count", placement.count());
            array.add(obj);
        }
        return array;
    }

    private JsonObject describeCraftingPlan(CraftingExecutionPlan plan) {
        JsonObject obj = new JsonObject();
        obj.addProperty("recipe", plan.recipeId());
        obj.addProperty("item", itemId(plan.result().getItem()));
        obj.addProperty("crafts", plan.crafts());
        obj.addProperty("output_count", plan.outputCount());
        obj.addProperty("requires_crafting_table", plan.requiresCraftingTable());
        obj.addProperty("using_crafting_table", plan.usingCraftingTable());
        obj.addProperty("container_id", plan.containerId());
        obj.add("ingredients", plan.ingredients());
        obj.add("missing", plan.missing());
        obj.add("placements", plan.placementsJson());
        return obj;
    }

    private void executeCraftingPlan(Minecraft mc, CraftingExecutionPlan plan) {
        if (mc.player == null || mc.gameMode == null || mc.player.containerMenu == null) {
            LOGGER.warn("Cannot execute crafting plan {}: player, game mode, or menu is unavailable", plan.recipeId());
            return;
        }
        AbstractContainerMenu menu = mc.player.containerMenu;
        if (menu.containerId != plan.containerId()) {
            LOGGER.warn("Cannot execute crafting plan {}: container changed from {} to {}",
                    plan.recipeId(), plan.containerId(), menu.containerId);
            return;
        }
        if (!menu.getCarried().isEmpty()) {
            LOGGER.warn("Cannot execute crafting plan {}: carried item is not empty", plan.recipeId());
            return;
        }

        for (int gridSlot : plan.gridSlots()) {
            if (menu.isValidSlotIndex(gridSlot) && menu.getSlot(gridSlot).hasItem()) {
                mc.gameMode.handleInventoryMouseClick(menu.containerId, gridSlot, 0, ClickType.QUICK_MOVE, mc.player);
            }
        }

        for (IngredientPlacement placement : plan.placements()) {
            for (int i = 0; i < placement.count(); i++) {
                if (!menu.isValidSlotIndex(placement.sourceSlot()) || !menu.isValidSlotIndex(placement.gridSlot())) {
                    LOGGER.warn("Skipping invalid crafting click for {}: source={}, grid={}",
                            plan.recipeId(), placement.sourceSlot(), placement.gridSlot());
                    return;
                }
                mc.gameMode.handleInventoryMouseClick(menu.containerId, placement.sourceSlot(), 0, ClickType.PICKUP, mc.player);
                mc.gameMode.handleInventoryMouseClick(menu.containerId, placement.gridSlot(), 1, ClickType.PICKUP, mc.player);
                mc.gameMode.handleInventoryMouseClick(menu.containerId, placement.sourceSlot(), 0, ClickType.PICKUP, mc.player);
            }
        }

        for (int i = 0; i < Math.max(1, plan.crafts()); i++) {
            mc.gameMode.handleInventoryMouseClick(menu.containerId, plan.resultSlot(), 0, ClickType.QUICK_MOVE, mc.player);
        }
    }

    private CraftingGridContext craftingGridContext(Minecraft mc) {
        if (mc.player == null || mc.player.containerMenu == null) {
            return null;
        }
        if (!(mc.player.containerMenu instanceof RecipeBookMenu<?, ?> recipeBookMenu)) {
            return null;
        }
        int resultSlot = recipeBookMenu.getResultSlotIndex();
        int size = recipeBookMenu.getSize();
        List<Integer> gridSlots = new ArrayList<>();
        for (int slot = 0; slot < size; slot++) {
            if (slot != resultSlot) {
                gridSlots.add(slot);
            }
        }
        return new CraftingGridContext(
                mc.player.containerMenu.containerId,
                resultSlot,
                recipeBookMenu.getGridWidth(),
                recipeBookMenu.getGridHeight(),
                gridSlots,
                mc.player.containerMenu instanceof CraftingMenu,
                mc.player.containerMenu instanceof InventoryMenu
        );
    }

    private JsonObject describeCraftingContext(Minecraft mc) {
        JsonObject obj = new JsonObject();
        CraftingGridContext context = craftingGridContext(mc);
        if (context == null) {
            obj.addProperty("available", false);
            obj.addProperty("reason", "no_crafting_recipe_book_menu");
            return obj;
        }
        obj.addProperty("available", true);
        obj.addProperty("container_id", context.containerId());
        obj.addProperty("grid_width", context.gridWidth());
        obj.addProperty("grid_height", context.gridHeight());
        obj.addProperty("result_slot", context.resultSlot());
        obj.addProperty("using_crafting_table", context.usingCraftingTable());
        obj.addProperty("using_player_inventory", context.usingPlayerInventory());
        JsonArray slots = new JsonArray();
        for (int slot : context.gridSlots()) {
            slots.add(slot);
        }
        obj.add("grid_slots", slots);
        return obj;
    }

    private boolean isSupportedCraftingRecipe(CraftingRecipe recipe) {
        return !recipe.isSpecial() && (recipe instanceof ShapedRecipe || recipe instanceof ShapelessRecipe);
    }

    private boolean requiresCraftingTable(CraftingRecipe recipe) {
        return !recipe.canCraftInDimensions(2, 2);
    }

    private String validateRecipeTemplate(RecipeHolder<CraftingRecipe> holder, int gridWidth, int gridHeight) {
        CraftingRecipe recipe = holder.value();
        if (!isSupportedCraftingRecipe(recipe)) {
            return "special_or_dynamic_recipe";
        }
        if (!recipe.canCraftInDimensions(gridWidth, gridHeight)) {
            return "does_not_fit_" + gridWidth + "x" + gridHeight;
        }
        if (recipe instanceof ShapedRecipe shapedRecipe) {
            return shapedRecipe.getIngredients().size() <= gridWidth * gridHeight ? null : "shape_slot_overflow";
        }
        return recipe.getIngredients().stream().filter(ingredient -> !ingredient.isEmpty()).count() <= (long) gridWidth * gridHeight
                ? null
                : "shapeless_slot_overflow";
    }

    private List<RecipeHolder<CraftingRecipe>> findCraftingRecipes(Minecraft mc, String target) {
        if (mc.level == null) {
            return List.of();
        }
        String normalized = canonicalCraftTarget(normalizeItemTarget(target));
        ResourceLocation directId = ResourceLocation.tryParse(target.contains(":") ? target : "minecraft:" + normalized);
        List<RecipeHolder<CraftingRecipe>> matches = new ArrayList<>();
        for (RecipeHolder<CraftingRecipe> holder : mc.level.getRecipeManager().getAllRecipesFor(RecipeType.CRAFTING)) {
            if (craftingRecipeMatchScore(mc, holder, directId, normalized) >= 0) {
                matches.add(holder);
            }
        }
        matches.sort(Comparator
                .comparingInt((RecipeHolder<CraftingRecipe> holder) -> craftingRecipeMatchScore(mc, holder, directId, normalized))
                .reversed()
                .thenComparing(holder -> holder.id().toString()));
        return matches;
    }

    private int craftingRecipeMatchScore(
            Minecraft mc,
            RecipeHolder<CraftingRecipe> holder,
            ResourceLocation directId,
            String normalized
    ) {
        String id = holder.id().toString().toLowerCase(Locale.ROOT);
        String path = holder.id().getPath().toLowerCase(Locale.ROOT);
        ItemStack result = holder.value().getResultItem(mc.level.registryAccess());
        String resultId = result.isEmpty() ? "" : itemId(result.getItem()).toLowerCase(Locale.ROOT);
        String resultPath = resultId.replace("minecraft:", "");

        if (directId != null && holder.id().equals(directId)) {
            return 100;
        }
        if (resultId.equals("minecraft:" + normalized) || resultPath.equals(normalized)) {
            return 90;
        }
        if (path.equals(normalized)) {
            return 80;
        }
        if (path.endsWith("/" + normalized) || path.endsWith("_" + normalized)) {
            return 70;
        }
        if (resultPath.contains(normalized)) {
            return 60;
        }
        if (path.contains(normalized)) {
            return 50;
        }
        return -1;
    }

    private String canonicalCraftTarget(String normalized) {
        if ("sticks".equals(normalized)) {
            return "stick";
        }
        if ("table".equals(normalized) || "craftingtable".equals(normalized)) {
            return "crafting_table";
        }
        if ("plank".equals(normalized)) {
            return "planks";
        }
        return normalized;
    }

    private String normalizeItemTarget(String target) {
        return target.toLowerCase(Locale.ROOT)
                .replace("minecraft:", "")
                .replace("minecraftbridge:", "")
                .replace("recipes/", "")
                .replace("tools/", "")
                .replace("misc/", "")
                .replace("decorations/", "")
                .replace("building_blocks/", "")
                .replace(" ", "_")
                .replace("-", "_");
    }

    private String itemId(Item item) {
        return BuiltInRegistries.ITEM.getKey(item).toString();
    }

    private BlockPos findNearestBlockById(Minecraft mc, String id, int requestedRadius) {
        int radius = Math.max(1, Math.min(MAX_SCAN_RADIUS, requestedRadius));
        BlockPos playerPos = mc.player.blockPosition();
        BlockPos nearest = null;
        double nearestDist = Double.MAX_VALUE;
        for (int shell = 0; shell <= radius; shell++) {
            for (int dx = -shell; dx <= shell; dx++) {
                for (int dy = -shell; dy <= shell; dy++) {
                    for (int dz = -shell; dz <= shell; dz++) {
                        if (Math.abs(dx) != shell && Math.abs(dy) != shell && Math.abs(dz) != shell) {
                            continue;
                        }
                        BlockPos pos = playerPos.offset(dx, dy, dz);
                        BlockState state = mc.level.getBlockState(pos);
                        if (id.equals(blockId(state))) {
                            double dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
                            if (dist < nearestDist) {
                                nearest = pos;
                                nearestDist = dist;
                            }
                        }
                    }
                }
            }
            if (nearest != null) {
                return nearest;
            }
        }
        return null;
    }

    private JsonObject describeContainer(Minecraft mc) {
        JsonObject container = new JsonObject();
        AbstractContainerMenu menu = mc.player.containerMenu;
        if (menu == null) {
            container.addProperty("open", false);
            return container;
        }

        container.addProperty("open", true);
        container.addProperty("container_id", menu.containerId);
        container.addProperty("menu_class", menu.getClass().getName());
        container.addProperty("is_inventory_menu", menu == mc.player.inventoryMenu);
        container.addProperty("screen_class", mc.screen == null ? "none" : mc.screen.getClass().getName());
        container.addProperty("slot_count", menu.slots.size());

        ItemStack carried = menu.getCarried();
        JsonObject carriedObj = new JsonObject();
        carriedObj.addProperty("item", carried.isEmpty() ? "empty" : itemId(carried.getItem()));
        carriedObj.addProperty("count", carried.getCount());
        container.add("carried", carriedObj);

        JsonArray slots = new JsonArray();
        for (int i = 0; i < menu.slots.size(); i++) {
            Slot slot = menu.slots.get(i);
            ItemStack stack = slot.getItem();
            JsonObject obj = new JsonObject();
            obj.addProperty("slot", i);
            obj.addProperty("container_slot", slot.getContainerSlot());
            obj.addProperty("x", slot.x);
            obj.addProperty("y", slot.y);
            obj.addProperty("active", slot.isActive());
            obj.addProperty("may_pickup", slot.mayPickup(mc.player));
            obj.addProperty("has_item", !stack.isEmpty());
            obj.addProperty("item", stack.isEmpty() ? "empty" : itemId(stack.getItem()));
            obj.addProperty("count", stack.getCount());
            if (!stack.isEmpty()) {
                obj.addProperty("display_name", stack.getHoverName().getString());
                obj.addProperty("max_stack_size", stack.getMaxStackSize());
            }
            slots.add(obj);
        }
        container.add("slots", slots);
        return container;
    }

    private JsonObject describeAdvancementProgress(AdvancementHolder holder, AdvancementProgress progress) {
        JsonObject obj = new JsonObject();
        obj.addProperty("id", holder.id().toString());
        obj.addProperty("done", progress.isDone());
        obj.addProperty("has_progress", progress.hasProgress());
        obj.addProperty("percent", progress.getPercent());
        JsonArray completedCriteria = new JsonArray();
        for (String criterion : progress.getCompletedCriteria()) {
            completedCriteria.add(criterion);
        }
        JsonArray remainingCriteria = new JsonArray();
        for (String criterion : progress.getRemainingCriteria()) {
            remainingCriteria.add(criterion);
        }
        obj.add("completed_criteria", completedCriteria);
        obj.add("remaining_criteria", remainingCriteria);
        return obj;
    }

    private Set<String> readCompletedAdvancementIds(Minecraft mc, int limit) {
        Set<String> ids = new HashSet<>();
        MinecraftServer server = mc.getSingleplayerServer();
        if (server == null) {
            return ids;
        }
        ServerPlayer serverPlayer = server.getPlayerList().getPlayer(mc.player.getUUID());
        if (serverPlayer == null) {
            return ids;
        }
        int count = 0;
        PlayerAdvancements playerAdvancements = serverPlayer.getAdvancements();
        for (AdvancementHolder holder : server.getAdvancements().getAllAdvancements()) {
            if (count++ >= limit) {
                break;
            }
            if (playerAdvancements.getOrStartProgress(holder).isDone()) {
                ids.add(holder.id().toString());
            }
        }
        return ids;
    }

    private JsonObject buildAdvancementCurriculum(Minecraft mc, Set<String> serverCompletedIds) {
        Set<String> completed = new HashSet<>(serverCompletedIds);
        JsonObject curriculum = new JsonObject();
        JsonArray milestones = new JsonArray();

        addCurriculumStep(mc, completed, milestones, "minecraft:story/root", "Open inventory",
                "scan_environment", "Establish baseline state and inventory visibility");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/mine_stone", "Mine stone",
                "mine_nearest_resource", "Collect cobblestone or cobbled deepslate");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/upgrade_tools", "Upgrade tools",
                "craft_stone_pickaxe", "Craft a stone pickaxe from cobblestone and sticks");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/smelt_iron", "Acquire hardware",
                "mine_nearest_resource", "Mine iron ore, craft a furnace, and smelt iron ingots");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/obtain_armor", "Suit up",
                "mine_nearest_resource", "Craft and equip any armor piece");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/lava_bucket", "Hot stuff",
                "mine_nearest_resource", "Craft a bucket and collect lava");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/iron_tools", "Isn't it iron pick",
                "mine_nearest_resource", "Craft an iron pickaxe");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/mine_diamond", "Diamonds",
                "mine_nearest_diamond", "Reach diamond depth, scan, mine, and collect diamonds");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/enter_the_nether", "Enter the Nether",
                "scan_environment", "Build or find a portal and enter the Nether");
        addCurriculumStep(mc, completed, milestones, "minecraft:nether/find_fortress", "Find a fortress",
                "sprint_wander", "Explore Nether biomes until a fortress is located");
        addCurriculumStep(mc, completed, milestones, "minecraft:nether/obtain_blaze_rod", "Into fire",
                "engage_hostile", "Defeat blazes and collect blaze rods");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/follow_ender_eye", "Eye spy",
                "sprint_wander", "Craft eyes of ender and locate a stronghold");
        addCurriculumStep(mc, completed, milestones, "minecraft:story/enter_the_end", "The End",
                "sprint_wander", "Activate the End portal and enter the End");
        addCurriculumStep(mc, completed, milestones, "minecraft:end/kill_dragon", "Free the End",
                "engage_hostile", "Defeat the ender dragon");
        addCurriculumStep(mc, completed, milestones, "minecraft:adventure/kill_all_mobs", "Monsters hunted",
                "engage_hostile", "Systematically encounter and defeat every hostile mob");
        addCurriculumStep(mc, completed, milestones, "minecraft:husbandry/balanced_diet", "Balanced diet",
                "sprint_wander", "Collect and eat every required food item");

        JsonArray targetIds = new JsonArray();
        JsonArray missingIds = new JsonArray();
        for (String id : VANILLA_ADVANCEMENT_IDS) {
            String fullId = "minecraft:" + id;
            targetIds.add(fullId);
            if (!completed.contains(fullId)) {
                missingIds.add(fullId);
            }
        }

        JsonObject next = null;
        for (JsonElement element : milestones) {
            JsonObject milestone = element.getAsJsonObject();
            if (!milestone.get("completed").getAsBoolean()) {
                next = milestone;
                break;
            }
        }

        curriculum.add("milestones", milestones);
        curriculum.add("all_vanilla_targets", targetIds);
        curriculum.add("missing_vanilla_targets", missingIds);
        curriculum.addProperty("target_count", targetIds.size());
        curriculum.addProperty("missing_target_count", missingIds.size());
        curriculum.addProperty("methodology",
                "safety -> wood -> crafting table -> stone tools -> iron -> diamonds -> nether -> blaze rods -> stronghold -> end -> exploration/combat/husbandry sweeps");
        if (next != null) {
            curriculum.add("next_milestone", next);
            curriculum.addProperty("recommended_skill", next.get("skill").getAsString());
        } else {
            curriculum.addProperty("recommended_skill", "scan_environment");
        }
        return curriculum;
    }

    private void addCurriculumStep(
            Minecraft mc,
            Set<String> serverCompleted,
            JsonArray milestones,
            String id,
            String title,
            String skill,
            String objective
    ) {
        boolean inferred = inferAdvancementComplete(mc, id);
        boolean completed = serverCompleted.contains(id) || inferred;
        if (completed) {
            serverCompleted.add(id);
        }
        JsonObject step = new JsonObject();
        step.addProperty("id", id);
        step.addProperty("title", title);
        step.addProperty("skill", skill);
        step.addProperty("objective", objective);
        step.addProperty("completed", completed);
        step.addProperty("inferred", inferred);
        milestones.add(step);
    }

    private boolean inferAdvancementComplete(Minecraft mc, String id) {
        Inventory inv = mc.player.getInventory();
        return switch (id) {
            case "minecraft:story/root" -> true;
            case "minecraft:story/mine_stone" ->
                    hasInventoryItemContaining(inv, "cobblestone", "cobbled_deepslate", "stone_pickaxe");
            case "minecraft:story/upgrade_tools" -> hasInventoryItemContaining(inv, "stone_pickaxe");
            case "minecraft:story/smelt_iron" -> hasInventoryItemContaining(inv, "iron_ingot");
            case "minecraft:story/obtain_armor" -> hasInventoryItemContaining(inv, "helmet", "chestplate", "leggings", "boots");
            case "minecraft:story/lava_bucket" -> hasInventoryItemContaining(inv, "lava_bucket");
            case "minecraft:story/iron_tools" -> hasInventoryItemContaining(inv, "iron_pickaxe");
            case "minecraft:story/mine_diamond" -> hasInventoryItemContaining(inv, "diamond");
            case "minecraft:story/enter_the_nether", "minecraft:nether/find_fortress", "minecraft:nether/obtain_blaze_rod" ->
                    mc.level.dimension().location().toString().contains("the_nether");
            case "minecraft:story/follow_ender_eye" -> hasInventoryItemContaining(inv, "ender_eye", "eye_of_ender");
            case "minecraft:story/enter_the_end", "minecraft:end/kill_dragon" ->
                    mc.level.dimension().location().toString().contains("the_end");
            default -> false;
        };
    }

    private boolean hasInventoryItemContaining(Inventory inv, String... terms) {
        for (int i = 0; i < inv.getContainerSize(); i++) {
            ItemStack stack = inv.getItem(i);
            if (stack.isEmpty()) {
                continue;
            }
            String id = itemId(stack.getItem());
            for (String term : terms) {
                if (id.contains(term)) {
                    return true;
                }
            }
        }
        return false;
    }

    private JsonObject executeMove(Minecraft mc, JsonObject cmd, long actionId) {
        float forward = clampFloat(getFloat(cmd, "forward", 0.0f), -1.0f, 1.0f);
        float strafe = clampFloat(getFloat(cmd, "strafe", 0.0f), -1.0f, 1.0f);
        double durationSeconds = getDouble(cmd, "duration", 0.0);

        actionQueue.offer(() -> {
            navigationState = null;
            controlState.forward = forward;
            controlState.strafe = strafe;
            controlState.useWorldMovement = false;
            controlState.expiresAtMs = durationSeconds > 0.0
                    ? System.currentTimeMillis() + (long) (durationSeconds * 1000.0)
                    : 0L;
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("forward", forward);
        response.addProperty("strafe", strafe);
        response.addProperty("duration", durationSeconds);
        return response;
    }

    private JsonObject executeLook(Minecraft mc, JsonObject cmd, long actionId) {
        float yaw = getFloat(cmd, "yaw", mc.player.getYRot());
        float pitch = clampFloat(getFloat(cmd, "pitch", mc.player.getXRot()), -90.0f, 90.0f);

        actionQueue.offer(() -> {
            mc.player.setYRot(yaw);
            mc.player.setXRot(Math.max(-90, Math.min(90, pitch)));
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("yaw", yaw);
        response.addProperty("pitch", pitch);
        return response;
    }

    private JsonObject executeJump(Minecraft mc, long actionId) {
        actionQueue.offer(() -> {
            if (mc.player.onGround()) {
                mc.player.jumpFromGround();
            }
        });
        return createSuccess(actionId);
    }

    private JsonObject executeSneak(Minecraft mc, JsonObject cmd, long actionId) {
        boolean sneaking = cmd.has("state") ? cmd.get("state").getAsBoolean() : true;
        actionQueue.offer(() -> controlState.sneaking = sneaking);
        JsonObject response = createSuccess(actionId);
        response.addProperty("sneaking", sneaking);
        return response;
    }

    private JsonObject executeSprint(Minecraft mc, JsonObject cmd, long actionId) {
        boolean sprinting = cmd.has("state") ? cmd.get("state").getAsBoolean() : true;
        actionQueue.offer(() -> controlState.sprinting = sprinting);
        JsonObject response = createSuccess(actionId);
        response.addProperty("sprinting", sprinting);
        return response;
    }

    private JsonObject executeSwim(Minecraft mc, long actionId) {
        actionQueue.offer(() -> controlState.swimming = true);
        return createSuccess(actionId);
    }

    private JsonObject executeStopMoving(Minecraft mc, long actionId) {
        actionQueue.offer(() -> {
            navigationState = null;
            controlState.clearMovement();
        });
        return createSuccess(actionId);
    }

    private JsonObject executeCrouch(Minecraft mc, JsonObject cmd, long actionId) {
        return executeSneak(mc, cmd, actionId);
    }

    private JsonObject executeTurn(Minecraft mc, JsonObject cmd, long actionId) {
        float deltaYaw = clampFloat(getFloat(cmd, "delta_yaw", 0.0f), -180.0f, 180.0f);
        float deltaPitch = clampFloat(getFloat(cmd, "delta_pitch", 0.0f), -90.0f, 90.0f);

        actionQueue.offer(() -> {
            float newYaw = mc.player.getYRot() + deltaYaw;
            float newPitch = Math.max(-90, Math.min(90, mc.player.getXRot() + deltaPitch));
            mc.player.setYRot(newYaw);
            mc.player.setXRot(newPitch);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("delta_yaw", deltaYaw);
        response.addProperty("delta_pitch", deltaPitch);
        return response;
    }

    private JsonObject executeAttack(Minecraft mc, JsonObject cmd, long actionId) {
        int requestedEntityId = getInt(cmd, "entity_id", Integer.MIN_VALUE);
        if (requestedEntityId != Integer.MIN_VALUE) {
            Entity target = mc.level.getEntity(requestedEntityId);
            if (target == null) {
                return createError("ENTITY_NOT_FOUND", "Entity not found", actionId);
            }
            if (mc.player.position().distanceTo(target.position()) > config.combatReachDistance + 1.0) {
                return createError("TARGET_OUT_OF_RANGE", "Target is outside combat reach", actionId);
            }
            actionQueue.offer(() -> {
                mc.gameMode.attack(mc.player, target);
                mc.player.swing(InteractionHand.MAIN_HAND);
            });
            JsonObject response = createSuccess(actionId);
            response.addProperty("entity_id", requestedEntityId);
            response.addProperty("target_type", entityId(target));
            response.addProperty("mode", "entity");
            return response;
        }

        actionQueue.offer(() -> {
            if (mc.hitResult != null && mc.hitResult.getType() == HitResult.Type.ENTITY) {
                EntityHitResult entityHit = (EntityHitResult) mc.hitResult;
                mc.gameMode.attack(mc.player, entityHit.getEntity());
                mc.player.swing(InteractionHand.MAIN_HAND);
            } else if (mc.hitResult != null && mc.hitResult.getType() == HitResult.Type.BLOCK) {
                BlockHitResult blockHit = (BlockHitResult) mc.hitResult;
                mc.gameMode.startDestroyBlock(blockHit.getBlockPos(), blockHit.getDirection());
            }
        });
        return createSuccess(actionId);
    }

    private JsonObject executeUse(Minecraft mc, JsonObject cmd, long actionId) {
        InteractionHand hand = cmd.has("off_hand") && cmd.get("off_hand").getAsBoolean() ?
                InteractionHand.OFF_HAND : InteractionHand.MAIN_HAND;

        actionQueue.offer(() -> mc.gameMode.useItem(mc.player, hand));

        JsonObject response = createSuccess(actionId);
        response.addProperty("hand", hand.toString());
        return response;
    }

    private JsonObject executeBlock(Minecraft mc, JsonObject cmd, long actionId) {
        boolean blocking = cmd.has("state") ? cmd.get("state").getAsBoolean() : true;
        actionQueue.offer(() -> {
            if (blocking) {
                mc.gameMode.useItem(mc.player, InteractionHand.OFF_HAND);
            }
        });
        JsonObject response = createSuccess(actionId);
        response.addProperty("blocking", blocking);
        return response;
    }

    private JsonObject targetEntity(Minecraft mc, JsonObject cmd, long actionId) {
        return aimAtEntity(mc, cmd, actionId);
    }

    private JsonObject aimAtEntity(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("entity_id")) {
            return createError("MISSING_PARAM", "entity_id required", actionId);
        }

        int entityId = cmd.get("entity_id").getAsInt();
        Entity target = mc.level.getEntity(entityId);

        if (target == null) {
            return createError("ENTITY_NOT_FOUND", "Entity not found", actionId);
        }

        Vec3 targetPos = targetPoint(target);
        Vec3 playerPos = mc.player.getEyePosition();
        double[] angles = yawPitch(playerPos, targetPos);
        double yaw = angles[0];
        double pitch = angles[1];

        actionQueue.offer(() -> {
            mc.player.setYRot((float) yaw);
            mc.player.setXRot((float) pitch);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("entity_id", entityId);
        response.addProperty("target_yaw", yaw);
        response.addProperty("target_pitch", pitch);
        response.addProperty("distance", mc.player.position().distanceTo(target.position()));
        response.addProperty("line_of_sight", hasLineOfSight(mc, target));
        return response;
    }

    private JsonObject getTarget(Minecraft mc, long actionId) {
        JsonObject response = createSuccess(actionId);

        if (mc.hitResult != null && mc.hitResult.getType() == HitResult.Type.ENTITY) {
            EntityHitResult entityHit = (EntityHitResult) mc.hitResult;
            Entity entity = entityHit.getEntity();
            response.addProperty("has_target", true);
            response.addProperty("entity_id", entity.getId());
            response.addProperty("entity_type", BuiltInRegistries.ENTITY_TYPE.getKey(entity.getType()).toString());
            if (entity instanceof LivingEntity living) {
                response.addProperty("health", living.getHealth());
                response.addProperty("max_health", living.getMaxHealth());
            }
        } else {
            response.addProperty("has_target", false);
        }

        return response;
    }

    private JsonObject executeFlee(Minecraft mc, JsonObject cmd, long actionId) {
        double radius = clampDouble(getDouble(cmd, "radius", config.threatAssessmentRadius), 4.0, MAX_SCAN_RADIUS);
        double durationSeconds = clampDouble(getDouble(cmd, "duration", 0.8), 0.1, 3.0);
        JsonObject threatAssessment = buildThreatAssessment(mc, cmd);
        double escapeYaw = threatAssessment.get("safe_escape_yaw").getAsDouble();

        actionQueue.offer(() -> {
            Vec3 playerPos = mc.player.position();
            Entity nearestThreat = null;
            double nearestDist = Double.MAX_VALUE;

            for (Entity entity : mc.level.getEntities(mc.player, mc.player.getBoundingBox().inflate(radius))) {
                if (isThreatEntity(entity)) {
                    double dist = playerPos.distanceTo(entity.position());
                    if (dist < nearestDist) {
                        nearestDist = dist;
                        nearestThreat = entity;
                    }
                }
            }

            if (nearestThreat != null) {
                mc.player.setYRot((float) escapeYaw);
            }
            navigationState = null;
            controlState.forward = 1.0f;
            controlState.strafe = 0.0f;
            controlState.useWorldMovement = false;
            controlState.sprinting = true;
            controlState.expiresAtMs = System.currentTimeMillis() + (long) (durationSeconds * 1000.0);
        });

        JsonObject response = createSuccess(actionId);
        response.add("threats", threatAssessment);
        response.addProperty("escape_yaw", escapeYaw);
        response.addProperty("duration", durationSeconds);
        return response;
    }

    private JsonObject executeAutoAttack(Minecraft mc, JsonObject cmd, long actionId) {
        double radius = clampDouble(getDouble(cmd, "radius", config.autoAttackRadius), 1.0, MAX_SCAN_RADIUS);
        boolean allowApproach = !cmd.has("approach") || cmd.get("approach").getAsBoolean();
        boolean selectWeapon = !cmd.has("select_weapon") || cmd.get("select_weapon").getAsBoolean();

        Entity target = findBestCombatTarget(mc, radius);
        if (target == null) {
            JsonObject response = createSuccess(actionId);
            response.addProperty("attacked", false);
            response.addProperty("reason", "no_target");
            response.add("combat", buildCombatState(mc));
            return response;
        }

        Vec3 targetPos = targetPoint(target);
        double[] angles = yawPitch(mc.player.getEyePosition(), targetPos);
        double distance = mc.player.position().distanceTo(target.position());
        boolean visible = hasLineOfSight(mc, target);
        int weaponSlot = findBestWeaponSlot(mc, true);
        boolean canSwing = visible && distance <= config.combatReachDistance
                && mc.player.getAttackStrengthScale(0.0f) >= 0.65f;

        actionQueue.offer(() -> {
            if (selectWeapon && weaponSlot >= 0) {
                mc.player.getInventory().selected = weaponSlot;
            }
            mc.player.setYRot((float) angles[0]);
            mc.player.setXRot((float) angles[1]);
            if (canSwing) {
                mc.gameMode.attack(mc.player, target);
                mc.player.swing(InteractionHand.MAIN_HAND);
            } else if (allowApproach && visible) {
                navigationState = null;
                controlState.forward = 1.0f;
                controlState.strafe = 0.0f;
                controlState.sprinting = distance > config.combatReachDistance + 1.5;
                controlState.expiresAtMs = System.currentTimeMillis() + 500L;
            }
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("target_id", target.getId());
        response.addProperty("target_type", entityId(target));
        response.addProperty("target_yaw", angles[0]);
        response.addProperty("target_pitch", angles[1]);
        response.addProperty("distance", distance);
        response.addProperty("visible", visible);
        response.addProperty("attacked", canSwing);
        response.addProperty("weapon_slot", weaponSlot);
        response.addProperty("approaching", !canSwing && allowApproach && visible);
        return response;
    }

    private JsonObject executeStopAttack(Minecraft mc, long actionId) {
        actionQueue.offer(() -> {
            controlState.clearMovement();
        });
        return createSuccess(actionId);
    }

    private JsonObject executeMine(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        BlockPos pos = new BlockPos(x, y, z);

        if (!validateCoordinates(mc, pos)) {
            return createError("INVALID_COORDS", "Coordinates out of bounds or unreachable", actionId);
        }

        Direction direction = parseDirection(cmd, "facing", Direction.UP);
        boolean newMining = !activeMining.containsKey(pos);
        activeMining.computeIfAbsent(pos, ignored -> new MiningState(pos, mc.level.getGameTime(), direction));

        actionQueue.offer(() -> {
            if (newMining) {
                mc.gameMode.startDestroyBlock(pos, direction);
            }
            mc.gameMode.continueDestroyBlock(pos, direction);
            mc.player.swing(InteractionHand.MAIN_HAND);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("status", "mining");
        response.addProperty("x", x);
        response.addProperty("y", y);
        response.addProperty("z", z);
        response.addProperty("facing", direction.toString());
        response.addProperty("progress", 0.0);
        return response;
    }

    private JsonObject executePlace(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        BlockPos pos = new BlockPos(x, y, z);

        if (!validateCoordinates(mc, pos)) {
            return createError("INVALID_COORDS", "Coordinates out of bounds or unreachable", actionId);
        }

        Direction facing = Direction.UP;
        if (cmd.has("facing")) {
            try {
                facing = Direction.valueOf(cmd.get("facing").getAsString().toUpperCase());
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Invalid facing direction, using UP");
            }
        }

        Direction finalFacing = facing;
        actionQueue.offer(() -> {
            BlockHitResult hitResult = new BlockHitResult(
                    Vec3.atCenterOf(pos), finalFacing, pos, false
            );
            mc.gameMode.useItemOn(mc.player, InteractionHand.MAIN_HAND, hitResult);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("x", x);
        response.addProperty("y", y);
        response.addProperty("z", z);
        response.addProperty("facing", facing.toString());
        return response;
    }

    private JsonObject getBlockInfo(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        BlockPos pos = new BlockPos(x, y, z);

        BlockState state = mc.level.getBlockState(pos);
        JsonObject response = createSuccess(actionId);
        response.addProperty("x", x);
        response.addProperty("y", y);
        response.addProperty("z", z);
        response.addProperty("block", BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString());
        response.addProperty("is_air", state.isAir());
        response.addProperty("block_light", mc.level.getBrightness(LightLayer.BLOCK, pos));
        response.addProperty("sky_light", mc.level.getBrightness(LightLayer.SKY, pos));

        return response;
    }

    private JsonObject checkToolValidity(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        BlockPos pos = new BlockPos(x, y, z);

        BlockState state = mc.level.getBlockState(pos);
        ItemStack tool = mc.player.getMainHandItem();

        JsonObject response = createSuccess(actionId);
        response.addProperty("can_harvest", tool.isCorrectToolForDrops(state));
        response.addProperty("requires_tool", state.requiresCorrectToolForDrops());
        response.addProperty("destroy_speed", tool.getDestroySpeed(state));
        response.addProperty("block", BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString());

        float destroySpeed = tool.getDestroySpeed(state);
        if (destroySpeed > 1.0f) {
            response.addProperty("estimated_ticks", (int) (state.getDestroySpeed(mc.level, pos) / destroySpeed * 20));
        }

        return response;
    }

    private JsonObject getMiningProgress(JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        BlockPos pos = new BlockPos(x, y, z);

        MiningState state = activeMining.get(pos);
        JsonObject response = createSuccess(actionId);

        if (state != null) {
            response.addProperty("is_mining", true);
            response.addProperty("progress", state.progress);
            response.addProperty("completed", state.completed);
        } else {
            response.addProperty("is_mining", false);
        }

        return response;
    }

    private JsonObject stopMining(JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        BlockPos pos = new BlockPos(x, y, z);

        activeMining.remove(pos);

        actionQueue.offer(() -> {
            Minecraft mc = Minecraft.getInstance();
            mc.gameMode.stopDestroyBlock();
        });

        return createSuccess(actionId);
    }

    private JsonObject executeSelectSlot(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("slot")) {
            return createError("MISSING_PARAM", "slot required", actionId);
        }

        int slot = cmd.get("slot").getAsInt();
        if (slot < 0 || slot > 8) {
            return createError("INVALID_SLOT", "Slot must be 0-8", actionId);
        }

        actionQueue.offer(() -> mc.player.getInventory().selected = slot);

        JsonObject response = createSuccess(actionId);
        response.addProperty("slot", slot);
        return response;
    }

    private JsonObject executeSwapSlots(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("slot1") || !cmd.has("slot2")) {
            return createError("MISSING_PARAM", "slot1 and slot2 required", actionId);
        }

        int slot1 = cmd.get("slot1").getAsInt();
        int slot2 = cmd.get("slot2").getAsInt();

        Inventory inv = mc.player.getInventory();
        if (slot1 < 0 || slot1 >= inv.getContainerSize() || slot2 < 0 || slot2 >= inv.getContainerSize()) {
            return createError("INVALID_SLOT", "Invalid slot number", actionId);
        }

        actionQueue.offer(() -> {
            ItemStack temp = inv.getItem(slot1).copy();
            inv.setItem(slot1, inv.getItem(slot2));
            inv.setItem(slot2, temp);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("slot1", slot1);
        response.addProperty("slot2", slot2);
        return response;
    }

    private JsonObject executeDropItem(Minecraft mc, JsonObject cmd, long actionId) {
        boolean dropAll = cmd.has("drop_all") && cmd.get("drop_all").getAsBoolean();
        int slot = cmd.has("slot") ? cmd.get("slot").getAsInt() : mc.player.getInventory().selected;

        actionQueue.offer(() -> {
            ItemStack itemToDrop = mc.player.getInventory().getItem(slot);
            if (!itemToDrop.isEmpty()) {
                mc.player.drop(itemToDrop, dropAll);
                if (dropAll) {
                    mc.player.getInventory().setItem(slot, ItemStack.EMPTY);
                }
            }
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("slot", slot);
        response.addProperty("drop_all", dropAll);
        return response;
    }

    private JsonObject executeEquipArmor(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("slot") || !cmd.has("item_slot")) {
            return createError("MISSING_PARAM", "slot and item_slot required", actionId);
        }

        String slotName = cmd.get("slot").getAsString().toUpperCase();
        int itemSlot = cmd.get("item_slot").getAsInt();

        EquipmentSlot equipSlot;
        try {
            equipSlot = EquipmentSlot.valueOf(slotName);
        } catch (IllegalArgumentException e) {
            return createError("INVALID_SLOT", "Invalid equipment slot", actionId);
        }

        actionQueue.offer(() -> {
            ItemStack item = mc.player.getInventory().getItem(itemSlot);
            mc.player.setItemSlot(equipSlot, item);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("equipment_slot", slotName);
        response.addProperty("item_slot", itemSlot);
        return response;
    }

    private JsonObject findItem(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("item")) {
            return createError("MISSING_PARAM", "item required", actionId);
        }

        String itemName = cmd.get("item").getAsString();
        Inventory inv = mc.player.getInventory();

        JsonArray foundSlots = new JsonArray();
        for (int i = 0; i < inv.getContainerSize(); i++) {
            ItemStack stack = inv.getItem(i);
            if (!stack.isEmpty() && BuiltInRegistries.ITEM.getKey(stack.getItem()).toString().contains(itemName)) {
                JsonObject slotInfo = new JsonObject();
                slotInfo.addProperty("slot", i);
                slotInfo.addProperty("count", stack.getCount());
                slotInfo.addProperty("item", BuiltInRegistries.ITEM.getKey(stack.getItem()).toString());
                foundSlots.add(slotInfo);
            }
        }

        JsonObject response = createSuccess(actionId);
        response.add("found_slots", foundSlots);
        response.addProperty("total_found", foundSlots.size());
        return response;
    }

    private JsonObject sortInventory(Minecraft mc, long actionId) {
        actionQueue.offer(() -> {
            // Basic sorting by item type
            Inventory inv = mc.player.getInventory();
            List<ItemStack> items = new ArrayList<>();
            for (int i = 9; i < inv.getContainerSize(); i++) {
                items.add(inv.getItem(i).copy());
                inv.setItem(i, ItemStack.EMPTY);
            }

            items.sort((a, b) -> {
                if (a.isEmpty() && b.isEmpty()) return 0;
                if (a.isEmpty()) return 1;
                if (b.isEmpty()) return -1;
                return BuiltInRegistries.ITEM.getKey(a.getItem()).toString()
                        .compareTo(BuiltInRegistries.ITEM.getKey(b.getItem()).toString());
            });

            for (int i = 0; i < items.size(); i++) {
                inv.setItem(i + 9, items.get(i));
            }
        });

        return createSuccess(actionId);
    }

    private JsonObject stackItems(Minecraft mc, long actionId) {
        actionQueue.offer(() -> {
            Inventory inv = mc.player.getInventory();
            for (int i = 0; i < inv.getContainerSize(); i++) {
                ItemStack stack1 = inv.getItem(i);
                if (stack1.isEmpty() || stack1.getCount() >= stack1.getMaxStackSize()) continue;

                for (int j = i + 1; j < inv.getContainerSize(); j++) {
                    ItemStack stack2 = inv.getItem(j);
                    if (ItemStack.isSameItemSameComponents(stack1, stack2)) {
                        int transfer = Math.min(stack2.getCount(), stack1.getMaxStackSize() - stack1.getCount());
                        stack1.grow(transfer);
                        stack2.shrink(transfer);
                        if (stack2.isEmpty()) {
                            inv.setItem(j, ItemStack.EMPTY);
                        }
                    }
                }
            }
        });

        return createSuccess(actionId);
    }

    private JsonObject eatFood(Minecraft mc, JsonObject cmd, long actionId) {
        boolean force = cmd.has("force") && cmd.get("force").getAsBoolean();
        if (!force && mc.player.getFoodData().getFoodLevel() >= 20 && mc.player.getHealth() >= mc.player.getMaxHealth()) {
            JsonObject response = createSuccess(actionId);
            response.addProperty("used", false);
            response.addProperty("reason", "not_hungry");
            response.addProperty("slot", -1);
            return response;
        }

        int slot = findBestFoodSlot(mc, true);
        if (slot < 0) {
            return createError("NO_FOOD", "No edible food found in hotbar", actionId);
        }

        ItemStack stack = mc.player.getInventory().getItem(slot);
        FoodProperties food = resolveFoodProperties(stack, mc.player);
        actionQueue.offer(() -> {
            mc.player.getInventory().selected = slot;
            mc.gameMode.useItem(mc.player, InteractionHand.MAIN_HAND);
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("used", true);
        response.addProperty("slot", slot);
        response.addProperty("item", BuiltInRegistries.ITEM.getKey(stack.getItem()).toString());
        if (food != null) {
            response.addProperty("nutrition", food.nutrition());
            response.addProperty("saturation", food.saturation());
        }
        return response;
    }

    private JsonObject selectBestTool(Minecraft mc, JsonObject cmd, long actionId) {
        String purpose = getString(cmd, "purpose", "weapon");
        int selectedSlot;
        JsonObject response = createSuccess(actionId);

        if (cmd.has("x") && cmd.has("y") && cmd.has("z")) {
            purpose = "tool";
            BlockPos pos = new BlockPos(cmd.get("x").getAsInt(), cmd.get("y").getAsInt(), cmd.get("z").getAsInt());
            BlockState state = mc.level.getBlockState(pos);
            selectedSlot = findBestToolSlotForBlock(mc, state, true);
            response.addProperty("block", blockId(state));
            response.addProperty("x", pos.getX());
            response.addProperty("y", pos.getY());
            response.addProperty("z", pos.getZ());
        } else if ("food".equalsIgnoreCase(purpose)) {
            selectedSlot = findBestFoodSlot(mc, true);
        } else {
            selectedSlot = findBestWeaponSlot(mc, true);
        }
        response.addProperty("purpose", purpose);

        if (selectedSlot < 0) {
            response.addProperty("selected", false);
            response.addProperty("slot", -1);
            return response;
        }

        int finalSelectedSlot = selectedSlot;
        actionQueue.offer(() -> mc.player.getInventory().selected = finalSelectedSlot);

        ItemStack stack = mc.player.getInventory().getItem(selectedSlot);
        response.addProperty("selected", true);
        response.addProperty("slot", selectedSlot);
        response.addProperty("item", stack.isEmpty() ? "empty" : BuiltInRegistries.ITEM.getKey(stack.getItem()).toString());
        return response;
    }

    private JsonObject interactEntity(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("entity_id")) {
            return createError("MISSING_PARAM", "entity_id required", actionId);
        }

        int entityId = cmd.get("entity_id").getAsInt();
        Entity entity = mc.level.getEntity(entityId);

        if (entity == null) {
            return createError("ENTITY_NOT_FOUND", "Entity not found", actionId);
        }

        actionQueue.offer(() -> mc.gameMode.interact(mc.player, entity, InteractionHand.MAIN_HAND));

        JsonObject response = createSuccess(actionId);
        response.addProperty("entity_id", entityId);
        response.addProperty("entity_type", BuiltInRegistries.ENTITY_TYPE.getKey(entity.getType()).toString());
        return response;
    }

    private JsonObject rideEntity(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("entity_id")) {
            return createError("MISSING_PARAM", "entity_id required", actionId);
        }

        int entityId = cmd.get("entity_id").getAsInt();
        Entity entity = mc.level.getEntity(entityId);

        if (entity == null) {
            return createError("ENTITY_NOT_FOUND", "Entity not found", actionId);
        }

        actionQueue.offer(() -> mc.player.startRiding(entity));

        JsonObject response = createSuccess(actionId);
        response.addProperty("entity_id", entityId);
        return response;
    }

    private JsonObject dismount(Minecraft mc, long actionId) {
        actionQueue.offer(() -> {
            if (mc.player.isPassenger()) {
                mc.player.stopRiding();
            }
        });

        return createSuccess(actionId);
    }

    private JsonObject tradeWithVillager(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("entity_id")) {
            return createError("MISSING_PARAM", "entity_id required", actionId);
        }

        int entityId = cmd.get("entity_id").getAsInt();
        Entity entity = mc.level.getEntity(entityId);

        if (!(entity instanceof Villager)) {
            return createError("NOT_VILLAGER", "Entity is not a villager", actionId);
        }

        actionQueue.offer(() -> mc.gameMode.interact(mc.player, entity, InteractionHand.MAIN_HAND));

        JsonObject response = createSuccess(actionId);
        response.addProperty("entity_id", entityId);
        response.addProperty("villager_type", ((Villager) entity).getVillagerData().getProfession().toString());
        return response;
    }

    private JsonObject feedEntity(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("entity_id")) {
            return createError("MISSING_PARAM", "entity_id required", actionId);
        }

        int entityId = cmd.get("entity_id").getAsInt();
        Entity entity = mc.level.getEntity(entityId);

        if (!(entity instanceof Animal)) {
            return createError("NOT_ANIMAL", "Entity is not feedable", actionId);
        }

        actionQueue.offer(() -> mc.gameMode.interact(mc.player, entity, InteractionHand.MAIN_HAND));

        JsonObject response = createSuccess(actionId);
        response.addProperty("entity_id", entityId);
        return response;
    }

    private JsonObject tameEntity(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("entity_id")) {
            return createError("MISSING_PARAM", "entity_id required", actionId);
        }

        int entityId = cmd.get("entity_id").getAsInt();
        Entity entity = mc.level.getEntity(entityId);

        if (entity == null) {
            return createError("ENTITY_NOT_FOUND", "Entity not found", actionId);
        }

        actionQueue.offer(() -> mc.gameMode.interact(mc.player, entity, InteractionHand.MAIN_HAND));

        JsonObject response = createSuccess(actionId);
        response.addProperty("entity_id", entityId);
        return response;
    }

    private JsonObject scanBlocks(Minecraft mc, JsonObject cmd, long actionId) {
        int radius = Math.max(1, Math.min(MAX_SCAN_RADIUS, getInt(cmd, "radius", 16)));
        String blockType = cmd.has("block_type") ? cmd.get("block_type").getAsString() : null;

        BlockPos playerPos = mc.player.blockPosition();
        JsonArray foundBlocks = new JsonArray();

        int count = 0;
        for (int x = -radius; x <= radius && count < MAX_NEARBY_BLOCKS; x++) {
            for (int y = -radius; y <= radius && count < MAX_NEARBY_BLOCKS; y++) {
                for (int z = -radius; z <= radius && count < MAX_NEARBY_BLOCKS; z++) {
                    BlockPos pos = playerPos.offset(x, y, z);
                    BlockState state = mc.level.getBlockState(pos);

                    if (!state.isAir()) {
                        String blockName = BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString();
                        if (blockType == null || blockName.contains(blockType)) {
                            foundBlocks.add(buildBlockStateObject(mc, pos, state, Math.sqrt(x * x + y * y + z * z)));
                            count++;
                        }
                    }
                }
            }
        }

        JsonObject response = createSuccess(actionId);
        response.add("blocks", foundBlocks);
        response.addProperty("total_found", foundBlocks.size());
        return response;
    }

    private JsonObject findNearestBlock(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("block_type")) {
            return createError("MISSING_PARAM", "block_type required", actionId);
        }

        String blockType = cmd.get("block_type").getAsString();
        String normalizedBlockType = blockType.toLowerCase(Locale.ROOT);
        int maxRadius = Math.max(1, Math.min(MAX_SCAN_RADIUS, getInt(cmd, "max_radius", 32)));
        boolean reachableOnly = getBoolean(cmd, "reachable_only", false);
        boolean exposedOnly = getBoolean(cmd, "exposed_only", false);
        boolean avoidUndermining = getBoolean(cmd, "avoid_undermining", normalizedBlockType.contains("log"));
        int maxVerticalDifference = clampInt(
                getInt(cmd, "max_vertical_difference", reachableOnly ? 4 : maxRadius),
                0,
                maxRadius
        );
        double maxReachCandidateDistance = clampDouble(
                getDouble(cmd, "max_reach_candidate_distance", config.maxReachDistance + 1.0),
                config.maxReachDistance,
                MAX_SCAN_RADIUS
        );

        BlockPos playerPos = mc.player.blockPosition();
        BlockPos nearest = null;
        double bestScore = Double.MAX_VALUE;
        double nearestDist = Double.MAX_VALUE;
        double nearestReachDist = Double.MAX_VALUE;
        boolean nearestExposed = false;

        for (int shell = 1; shell <= maxRadius; shell++) {
            for (int dx = -shell; dx <= shell; dx++) {
                for (int dy = -shell; dy <= shell; dy++) {
                    for (int dz = -shell; dz <= shell; dz++) {
                        if (Math.abs(dx) == shell || Math.abs(dy) == shell || Math.abs(dz) == shell) {
                            BlockPos pos = playerPos.offset(dx, dy, dz);
                            BlockState state = mc.level.getBlockState(pos);
                            String blockName = BuiltInRegistries.BLOCK.getKey(state.getBlock()).toString();

                            if (blockName.toLowerCase(Locale.ROOT).contains(normalizedBlockType)) {
                                if (avoidUndermining && wouldUnderminePlayer(mc, pos)) {
                                    continue;
                                }
                                int verticalDelta = Math.abs(pos.getY() - playerPos.getY());
                                if (verticalDelta > maxVerticalDifference) {
                                    continue;
                                }
                                boolean exposed = hasAirNeighbor(mc, pos);
                                if (exposedOnly && !exposed) {
                                    continue;
                                }
                                double reachDist = mc.player.getEyePosition().distanceTo(Vec3.atCenterOf(pos));
                                if (reachableOnly && reachDist > maxReachCandidateDistance) {
                                    continue;
                                }
                                double horizontalDist = Math.sqrt(dx * dx + dz * dz);
                                double worldDist = mc.player.position().distanceTo(Vec3.atCenterOf(pos));
                                double verticalPenalty = Math.max(0, verticalDelta - 1) * 3.0;
                                double hiddenPenalty = exposed ? 0.0 : 6.0;
                                double score = worldDist + horizontalDist * 0.15 + verticalPenalty + hiddenPenalty;
                                if (score < bestScore) {
                                    bestScore = score;
                                    nearestDist = worldDist;
                                    nearestReachDist = reachDist;
                                    nearestExposed = exposed;
                                    nearest = pos;
                                }
                            }
                        }
                    }
                }
            }
            if (nearest != null && bestScore <= config.maxReachDistance + 1.0) break;
        }

        JsonObject response = createSuccess(actionId);
        if (nearest != null) {
            response.addProperty("found", true);
            response.addProperty("x", nearest.getX());
            response.addProperty("y", nearest.getY());
            response.addProperty("z", nearest.getZ());
            response.addProperty("distance", nearestDist);
            response.addProperty("reach_distance", nearestReachDist);
            response.addProperty("vertical_delta", Math.abs(nearest.getY() - playerPos.getY()));
            response.addProperty("exposed", nearestExposed);
            response.addProperty("block", BuiltInRegistries.BLOCK.getKey(mc.level.getBlockState(nearest).getBlock()).toString());
        } else {
            response.addProperty("found", false);
            response.addProperty("reachable_only", reachableOnly);
            response.addProperty("exposed_only", exposedOnly);
            response.addProperty("max_vertical_difference", maxVerticalDifference);
        }

        return response;
    }

    private boolean hasAirNeighbor(Minecraft mc, BlockPos pos) {
        for (Direction direction : Direction.values()) {
            if (mc.level.getBlockState(pos.relative(direction)).isAir()) {
                return true;
            }
        }
        return false;
    }

    private JsonObject getBiome(Minecraft mc, JsonObject cmd, long actionId) {
        int x = cmd.has("x") ? cmd.get("x").getAsInt() : mc.player.blockPosition().getX();
        int y = cmd.has("y") ? cmd.get("y").getAsInt() : mc.player.blockPosition().getY();
        int z = cmd.has("z") ? cmd.get("z").getAsInt() : mc.player.blockPosition().getZ();

        BlockPos pos = new BlockPos(x, y, z);
        Holder<Biome> biome = mc.level.getBiome(pos);

        JsonObject response = createSuccess(actionId);
        response.addProperty("x", x);
        response.addProperty("y", y);
        response.addProperty("z", z);
        response.addProperty("biome", biome.unwrapKey()
                .map(key -> key.location().toString())
                .orElse("unknown"));
        return response;
    }

    private JsonObject getLightLevel(Minecraft mc, JsonObject cmd, long actionId) {
        int x = cmd.has("x") ? cmd.get("x").getAsInt() : mc.player.blockPosition().getX();
        int y = cmd.has("y") ? cmd.get("y").getAsInt() : mc.player.blockPosition().getY();
        int z = cmd.has("z") ? cmd.get("z").getAsInt() : mc.player.blockPosition().getZ();

        BlockPos pos = new BlockPos(x, y, z);

        JsonObject response = createSuccess(actionId);
        response.addProperty("x", x);
        response.addProperty("y", y);
        response.addProperty("z", z);
        response.addProperty("block_light", mc.level.getBrightness(LightLayer.BLOCK, pos));
        response.addProperty("sky_light", mc.level.getBrightness(LightLayer.SKY, pos));
        response.addProperty("combined_light", mc.level.getMaxLocalRawBrightness(pos));
        return response;
    }

    private JsonObject performRaycast(Minecraft mc, JsonObject cmd, long actionId) {
        JsonObject response = createSuccess(actionId);
        float yaw = getFloat(cmd, "yaw", mc.player.getYRot());
        float pitch = clampFloat(getFloat(cmd, "pitch", mc.player.getXRot()), -90.0f, 90.0f);
        double distance = clampDouble(getDouble(cmd, "distance", config.maxReachDistance), 0.5, config.maxRaycastDistance);
        JsonObject raycast = describeRaycast(mc, yaw, pitch, distance);
        for (Map.Entry<String, JsonElement> entry : raycast.entrySet()) {
            response.add(entry.getKey(), entry.getValue());
        }
        return response;
    }

    private JsonObject markLocation(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("name")) {
            return createError("MISSING_PARAM", "name required", actionId);
        }

        String name = cmd.get("name").getAsString();
        int x = cmd.has("x") ? cmd.get("x").getAsInt() : mc.player.blockPosition().getX();
        int y = cmd.has("y") ? cmd.get("y").getAsInt() : mc.player.blockPosition().getY();
        int z = cmd.has("z") ? cmd.get("z").getAsInt() : mc.player.blockPosition().getZ();

        BlockPos pos = new BlockPos(x, y, z);
        waypoints.put(name, pos);

        JsonObject response = createSuccess(actionId);
        response.addProperty("name", name);
        response.addProperty("x", x);
        response.addProperty("y", y);
        response.addProperty("z", z);
        return response;
    }

    private JsonObject getWaypoints(long actionId) {
        JsonObject response = createSuccess(actionId);
        JsonArray waypointsArray = new JsonArray();

        waypoints.forEach((name, pos) -> {
            JsonObject waypoint = new JsonObject();
            waypoint.addProperty("name", name);
            waypoint.addProperty("x", pos.getX());
            waypoint.addProperty("y", pos.getY());
            waypoint.addProperty("z", pos.getZ());
            waypointsArray.add(waypoint);
        });

        response.add("waypoints", waypointsArray);
        response.addProperty("total", waypoints.size());
        return response;
    }

    private JsonObject deleteWaypoint(JsonObject cmd, long actionId) {
        if (!cmd.has("name")) {
            return createError("MISSING_PARAM", "name required", actionId);
        }

        String name = cmd.get("name").getAsString();
        BlockPos removed = waypoints.remove(name);

        JsonObject response = createSuccess(actionId);
        response.addProperty("deleted", removed != null);
        response.addProperty("name", name);
        return response;
    }

    private JsonObject navigateTo(Minecraft mc, JsonObject cmd, long actionId) {
        if (!cmd.has("x") || !cmd.has("y") || !cmd.has("z")) {
            return createError("MISSING_COORDS", "x, y, z coordinates required", actionId);
        }

        int x = cmd.get("x").getAsInt();
        int y = cmd.get("y").getAsInt();
        int z = cmd.get("z").getAsInt();
        double stopDistance = clampDouble(getDouble(cmd, "stop_distance", 1.6), 0.5, 5.0);
        double timeoutSeconds = clampDouble(getDouble(cmd, "timeout", 12.0), 1.0, 120.0);
        boolean sprint = !cmd.has("sprint") || cmd.get("sprint").getAsBoolean();

        Vec3 target = new Vec3(x + 0.5, y, z + 0.5);
        Vec3 playerPos = mc.player.position();
        Vec3 direction = target.subtract(playerPos).normalize();

        double yaw = Math.toDegrees(Math.atan2(-direction.x, direction.z));
        double distance = playerPos.distanceTo(target);

        actionQueue.offer(() -> {
            navigationState = new NavigationState(target, stopDistance, timeoutSeconds, sprint);
            mc.player.setYRot((float) yaw);
            controlState.forward = 1.0f;
            controlState.strafe = 0.0f;
            controlState.sprinting = sprint;
            controlState.expiresAtMs = 0L;
        });

        JsonObject response = createSuccess(actionId);
        response.addProperty("target_x", x);
        response.addProperty("target_y", y);
        response.addProperty("target_z", z);
        response.addProperty("distance", distance);
        response.addProperty("heading", yaw);
        response.addProperty("stop_distance", stopDistance);
        response.addProperty("timeout", timeoutSeconds);
        response.addProperty("sprint", sprint);
        return response;
    }

    private JsonObject executeHarvest(Minecraft mc, JsonObject cmd, long actionId) {
        try {
            String targetResource = getString(cmd, "resource", "log").toLowerCase(Locale.ROOT);
            int radius = clampInt(getInt(cmd, "radius", 24), 1, MAX_SCAN_RADIUS);
            String blockType = harvestResourceBlockType(targetResource);
            BlockPos target = findHarvestTarget(mc, blockType, radius);

            JsonObject response = createSuccess(actionId);
            response.addProperty("resource", targetResource);
            response.addProperty("block_type", blockType);
            response.addProperty("radius", radius);

            if (target == null) {
                response.addProperty("found", false);
                response.addProperty("phase", "scan");
                response.addProperty("status", "not_found");
                return response;
            }

            BlockState targetState = mc.level.getBlockState(target);
            double distance = mc.player.position().distanceTo(Vec3.atCenterOf(target));
            double reachDistance = mc.player.getEyePosition().distanceTo(Vec3.atCenterOf(target));

            response.addProperty("found", true);
            response.addProperty("x", target.getX());
            response.addProperty("y", target.getY());
            response.addProperty("z", target.getZ());
            response.addProperty("distance", distance);
            response.addProperty("reach_distance", reachDistance);
            response.addProperty("block", blockId(targetState));

            if (reachDistance > config.maxReachDistance) {
                Vec3 targetVec = Vec3.atCenterOf(target);
                Vec3 playerPos = mc.player.position();
                Vec3 direction = targetVec.subtract(playerPos).normalize();
                double yaw = Math.toDegrees(Math.atan2(-direction.x, direction.z));
                double stopDistance = clampDouble(getDouble(cmd, "stop_distance", 3.8), 1.5, config.maxReachDistance);
                double timeoutSeconds = clampDouble(getDouble(cmd, "timeout", 12.0), 1.0, 120.0);

                actionQueue.offer(() -> {
                    navigationState = new NavigationState(targetVec, stopDistance, timeoutSeconds, true);
                    mc.player.setYRot((float) yaw);
                    controlState.forward = 1.0f;
                    controlState.strafe = 0.0f;
                    controlState.sprinting = distance > 8.0;
                    controlState.expiresAtMs = 0L;
                });

                response.addProperty("phase", "approach");
                response.addProperty("status", "navigating");
                response.addProperty("stop_distance", stopDistance);
                response.addProperty("timeout", timeoutSeconds);
                return response;
            }

            int toolSlot = findBestToolSlotForBlock(mc, targetState, true);
            Vec3 eye = mc.player.getEyePosition();
            double[] angles = yawPitch(eye, Vec3.atCenterOf(target));
            Direction direction = Direction.getNearest(
                    target.getX() + 0.5 - eye.x,
                    target.getY() + 0.5 - eye.y,
                    target.getZ() + 0.5 - eye.z
            ).getOpposite();
            boolean newMining = !activeMining.containsKey(target);
            activeMining.computeIfAbsent(target, ignored -> new MiningState(target, mc.level.getGameTime(), direction));

            actionQueue.offer(() -> {
                if (toolSlot >= 0) {
                    mc.player.getInventory().selected = toolSlot;
                }
                mc.player.setYRot((float) angles[0]);
                mc.player.setXRot((float) angles[1]);
                if (newMining) {
                    mc.gameMode.startDestroyBlock(target, direction);
                }
                mc.gameMode.continueDestroyBlock(target, direction);
                mc.player.swing(InteractionHand.MAIN_HAND);
            });

            response.addProperty("phase", "mine");
            response.addProperty("status", "mining");
            response.addProperty("tool_slot", toolSlot);
            return response;

        } catch (Exception e) {
            LOGGER.error("Error processing harvest command: {}", e.getMessage(), e);
            return createError("HARVEST_ERROR", "Failed to execute harvest: " + e.getMessage(), actionId);
        }
    }

    private BlockPos findHarvestTarget(Minecraft mc, String blockType, int radius) {
        BlockPos playerPos = mc.player.blockPosition();
        BlockPos nearest = null;
        double bestScore = Double.MAX_VALUE;

        for (int shell = 1; shell <= radius; shell++) {
            for (int dx = -shell; dx <= shell; dx++) {
                for (int dy = -shell; dy <= shell; dy++) {
                    for (int dz = -shell; dz <= shell; dz++) {
                        if (Math.abs(dx) != shell && Math.abs(dy) != shell && Math.abs(dz) != shell) {
                            continue;
                        }

                        BlockPos pos = playerPos.offset(dx, dy, dz);
                        BlockState state = mc.level.getBlockState(pos);
                        String blockName = blockId(state);
                        if (!blockName.contains(blockType)) {
                            continue;
                        }
                        if (wouldUnderminePlayer(mc, pos)) {
                            continue;
                        }
                        if (!isHarvestReachableCandidate(mc, pos, playerPos)) {
                            continue;
                        }

                        double distance = mc.player.position().distanceTo(Vec3.atCenterOf(pos));
                        double verticalPenalty = Math.max(0, Math.abs(pos.getY() - playerPos.getY()) - 1) * 3.0;
                        double score = distance + verticalPenalty;
                        if (score < bestScore) {
                            bestScore = score;
                            nearest = pos;
                        }
                    }
                }
            }
            if (nearest != null && bestScore <= config.maxReachDistance + 1.0) {
                break;
            }
        }

        return nearest;
    }

    private boolean isHarvestReachableCandidate(Minecraft mc, BlockPos pos, BlockPos playerPos) {
        int verticalDelta = pos.getY() - playerPos.getY();
        if (verticalDelta > 4 || verticalDelta < -3) {
            return false;
        }
        if (mc.player.getEyePosition().distanceTo(Vec3.atCenterOf(pos)) > config.maxReachDistance + 0.75) {
            return false;
        }
        return hasAirNeighbor(mc, pos);
    }

    private boolean wouldUnderminePlayer(Minecraft mc, BlockPos pos) {
        Vec3 playerPos = mc.player.position();
        double dx = Math.abs((pos.getX() + 0.5) - playerPos.x);
        double dz = Math.abs((pos.getZ() + 0.5) - playerPos.z);
        int feetY = (int) Math.floor(playerPos.y - 0.01);
        return dx < 0.9 && dz < 0.9 && pos.getY() <= feetY;
    }

    private String harvestResourceBlockType(String resource) {
        String normalized = resource == null ? "log" : resource.toLowerCase(Locale.ROOT);
        if (normalized.contains("wood") || normalized.contains("tree") || normalized.contains("log")) {
            return "log";
        }
        if (normalized.contains("stone") || normalized.contains("cobble")) {
            return "stone";
        }
        if (normalized.contains("diamond")) {
            return "diamond_ore";
        }
        if (normalized.contains("iron")) {
            return "iron_ore";
        }
        if (normalized.contains("coal")) {
            return "coal_ore";
        }
        return normalized.replace("minecraft:", "");
    }

    private JsonObject getAIStatus(long actionId) {
        JsonObject response = createSuccess(actionId);
        JsonObject ai = new JsonObject();
        MinecraftAIManager.getStatistics().forEach((key, value) -> addJsonValue(ai, key, value));
        response.add("ai", ai);
        response.addProperty("force_unpause", UnpauseHandler.isForceUnpauseEnabled());
        response.addProperty("pause_state", UnpauseHandler.getPauseStateInfo());
        return response;
    }

    private JsonObject pauseHarvest(JsonObject cmd, long actionId) {
        boolean paused = !cmd.has("state") || cmd.get("state").getAsBoolean();
        MinecraftAIManager.setPaused(paused);
        JsonObject response = createSuccess(actionId);
        response.addProperty("paused", paused);
        return response;
    }

    private JsonObject resumeHarvest(long actionId) {
        MinecraftAIManager.setPaused(false);
        JsonObject response = createSuccess(actionId);
        response.addProperty("paused", false);
        return response;
    }

    private JsonObject stopHarvest(long actionId) {
        MinecraftAIManager.stopHarvesting();
        JsonObject response = createSuccess(actionId);
        response.addProperty("running", MinecraftAIManager.isRunning());
        return response;
    }

    private JsonObject setUnpause(JsonObject cmd, long actionId) {
        boolean enabled = !cmd.has("state") || cmd.get("state").getAsBoolean();
        if (enabled) {
            UnpauseHandler.enableForceUnpause();
        } else {
            UnpauseHandler.disableForceUnpause();
        }
        JsonObject response = createSuccess(actionId);
        response.addProperty("force_unpause", enabled);
        return response;
    }

    private void addJsonValue(JsonObject object, String key, Object value) {
        if (value == null) {
            object.addProperty(key, "null");
        } else if (value instanceof Number number) {
            object.addProperty(key, number);
        } else if (value instanceof Boolean bool) {
            object.addProperty(key, bool);
        } else {
            object.addProperty(key, value.toString());
        }
    }

    private JsonObject getServerUptime(long actionId) {
        JsonObject response = createSuccess(actionId);
        long uptimeMs = System.currentTimeMillis() - serverStartTime;
        response.addProperty("uptime_ms", uptimeMs);
        response.addProperty("uptime_seconds", uptimeMs / 1000);
        response.addProperty("uptime_minutes", uptimeMs / 60000);
        response.addProperty("start_time", serverStartTime);
        return response;
    }

    private JsonObject stopServerCommand(long actionId) {
        JsonObject response = createSuccess(actionId);
        response.addProperty("message", "Server shutdown initiated");

        new Thread(() -> {
            try {
                Thread.sleep(1000);
                stopServer();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "BridgeServer-Shutdown").start();

        return response;
    }

    private record CraftingGridContext(
            int containerId,
            int resultSlot,
            int gridWidth,
            int gridHeight,
            List<Integer> gridSlots,
            boolean usingCraftingTable,
            boolean usingPlayerInventory
    ) {
        int gridSlot(int col, int row) {
            return gridSlots.get(col + row * gridWidth);
        }
    }

    private record IngredientTarget(
            int ingredientIndex,
            Ingredient ingredient,
            int gridSlot,
            int gridCol,
            int gridRow
    ) {}

    private record IngredientPlacement(
            int ingredientIndex,
            int sourceSlot,
            int sourceContainerSlot,
            int gridSlot,
            int gridCol,
            int gridRow,
            String itemId,
            int count
    ) {}

    private record AssignmentResult(
            boolean success,
            List<IngredientPlacement> placements,
            JsonArray ingredientsJson,
            JsonArray missing,
            JsonArray placementsJson
    ) {}

    private record CraftingExecutionPlan(
            String recipeId,
            ItemStack result,
            int outputCount,
            int crafts,
            boolean requiresCraftingTable,
            boolean usingCraftingTable,
            int containerId,
            int resultSlot,
            List<Integer> gridSlots,
            List<IngredientPlacement> placements,
            JsonArray ingredients,
            JsonArray missing,
            JsonArray placementsJson
    ) {}

    private record CraftingPlanResult(
            CraftingExecutionPlan plan,
            String code,
            String message,
            RecipeHolder<CraftingRecipe> recipe,
            JsonArray missing
    ) {}

    /**
     * Internal class representing a connected client handler.
     */
    private class ClientHandler implements Runnable {
        private final Socket socket;
        private BufferedReader in;
        private PrintWriter out;
        private final AtomicBoolean connected;
        private final RateLimiter rateLimiter;

        public ClientHandler(Socket socket) {
            this.socket = socket;
            this.connected = new AtomicBoolean(true);
            this.rateLimiter = new RateLimiter(config.commandRateLimit, config.commandBurst);
        }

        @Override
        public void run() {
            try {
                in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);

                if (authToken != null) {
                    String authLine = in.readLine();
                    JsonObject authCmd = JsonParser.parseString(authLine).getAsJsonObject();
                    if (!authCmd.has("auth") || !authToken.equals(authCmd.get("auth").getAsString())) {
                        JsonObject error = new JsonObject();
                        error.addProperty("status", "error");
                        error.addProperty("message", "Authentication failed");
                        sendMessage(gson.toJson(error) + "\n");
                        disconnect();
                        return;
                    }
                }

                JsonObject welcome = new JsonObject();
                welcome.addProperty("status", "connected");
                welcome.addProperty("version", BRIDGE_VERSION);
                sendMessage(gson.toJson(welcome) + "\n");

                String line;
                while (connected.get() && (line = in.readLine()) != null) {
                    if (!rateLimiter.allowRequest()) {
                        JsonObject error = new JsonObject();
                        error.addProperty("status", "error");
                        error.addProperty("message", "Rate limit exceeded");
                        sendMessage(gson.toJson(error) + "\n");
                        continue;
                    }

                    JsonObject cmd = null;
                    try {
                        JsonElement parsed = JsonParser.parseString(line);
                        if (!parsed.isJsonObject()) {
                            throw new IllegalArgumentException("command must be a JSON object");
                        }
                        cmd = parsed.getAsJsonObject();
                        JsonObject response = processCommand(cmd, this);
                        sendMessage(gson.toJson(response) + "\n");
                    } catch (Exception e) {
                        JsonObject error = new JsonObject();
                        error.addProperty("status", "error");
                        error.addProperty("message", "Invalid JSON: " + e.getMessage());
                        if (cmd != null) {
                            echoRequestId(cmd, error);
                        }
                        sendMessage(gson.toJson(error) + "\n");
                    } catch (Throwable e) {
                        rethrowIfCritical(e);
                        LOGGER.error("Fatal bridge command failure: {}", e.getMessage(), e);
                        JsonObject error = createError("COMMAND_FAILURE", describeThrowable(e), -1L);
                        if (cmd != null) {
                            echoRequestId(cmd, error);
                        }
                        sendMessage(gson.toJson(error) + "\n");
                    }
                }

            } catch (IOException e) {
                if (connected.get()) {
                    LOGGER.debug("Client I/O error: {}", e.getMessage());
                }
            } finally {
                disconnect();
            }
        }

        public synchronized void sendMessage(String message) {
            if (connected.get() && out != null) {
                out.print(message);
                out.flush();
            }
        }

        public void disconnect() {
            if (!connected.getAndSet(false)) {
                return;
            }

            activeClients.remove(this);

            try {
                if (in != null) in.close();
                if (out != null) out.close();
                if (!socket.isClosed()) socket.close();
            } catch (IOException e) {
                LOGGER.debug("Error closing client connection: {}", e.getMessage());
            }

            LOGGER.info("Client disconnected: {}", socket.getRemoteSocketAddress());
        }
    }

    /**
     * Simple token bucket rate limiter for command throttling.
     */
    private static class RateLimiter {
        private final int maxTokens;
        private final int refillRate;
        private int tokens;
        private long lastRefill;

        public RateLimiter(int refillRate, int maxTokens) {
            this.refillRate = refillRate;
            this.maxTokens = maxTokens;
            this.tokens = maxTokens;
            this.lastRefill = System.currentTimeMillis();
        }

        public synchronized boolean allowRequest() {
            refill();
            if (tokens > 0) {
                tokens--;
                return true;
            }
            return false;
        }

        private void refill() {
            long now = System.currentTimeMillis();
            long elapsed = now - lastRefill;
            int tokensToAdd = (int) (elapsed * refillRate / 1000);

            if (tokensToAdd > 0) {
                tokens = Math.min(maxTokens, tokens + tokensToAdd);
                lastRefill = now;
            }
        }
    }

    /**
     * Opens a local world only after the title-screen client has had time to finish
     * resource setup. This avoids the Forge quick-play startup path that can race
     * resource reloads in dev clients.
     */
    private class DelayedWorldOpen implements Runnable {
        private final String worldName;
        private final int minReadinessChecks;
        private final int maxReadinessChecks;
        private int readinessChecks;

        private DelayedWorldOpen(String worldName, int minReadinessChecks, int maxReadinessChecks) {
            this.worldName = worldName;
            this.minReadinessChecks = minReadinessChecks;
            this.maxReadinessChecks = maxReadinessChecks;
        }

        @Override
        public void run() {
            Minecraft mc = Minecraft.getInstance();

            readinessChecks++;
            boolean titleOrMenuReady = mc.screen != null;
            if ((readinessChecks < minReadinessChecks || !titleOrMenuReady) && readinessChecks < maxReadinessChecks) {
                reschedule("waiting for title/menu readiness");
                return;
            }

            try {
                LOGGER.info("Opening singleplayer world '{}' after {} readiness checks", worldName, readinessChecks);
                mc.createWorldOpenFlows().openWorld(worldName, () ->
                        LOGGER.info("Singleplayer world '{}' open flow completed", worldName)
                );
            } catch (Throwable e) {
                rethrowIfCritical(e);
                LOGGER.error("Failed to open singleplayer world '{}': {}", worldName, e.getMessage(), e);
            }
        }

        private void reschedule(String reason) {
            if (readinessChecks >= maxReadinessChecks) {
                LOGGER.error("Giving up opening singleplayer world '{}' after {} checks: {}", worldName, readinessChecks, reason);
                return;
            }
            readinessChecks++;
            scheduleWorldOpen(this, 50L);
        }
    }

    /**
     * Record of a completed action for history tracking.
     */
    private record ActionRecord(String action, long timestamp) {}

    /**
     * Desired held controls. Commands update this state once; the client tick loop
     * reapplies it every tick so short bridge requests behave like held keys.
     */
    private static class ControlState {
        public float forward;
        public float strafe;
        public boolean sprinting;
        public boolean sneaking;
        public boolean swimming;
        public boolean useWorldMovement;
        public double worldMovementX;
        public double worldMovementZ;
        public long expiresAtMs;

        public void clearMovement() {
            forward = 0.0f;
            strafe = 0.0f;
            sprinting = false;
            swimming = false;
            useWorldMovement = false;
            worldMovementX = 0.0;
            worldMovementZ = 0.0;
            expiresAtMs = 0L;
        }
    }

    /**
     * Lightweight navigation intent for bridge-level movement. It is deliberately
     * simple and player-like: turn toward the target, hold forward/sprint, and jump
     * when blocked.
     */
    private static class NavigationState {
        public final Vec3 target;
        public final double stopDistance;
        public final long timeoutMs;
        public final long startedAtMs;
        public final boolean sprint;
        public Vec3 lastPos;
        public long lastProgressCheckMs;
        public int stuckChecks;

        public NavigationState(Vec3 target, double stopDistance, double timeoutSeconds, boolean sprint) {
            this.target = target;
            this.stopDistance = stopDistance;
            this.timeoutMs = (long) (timeoutSeconds * 1000.0);
            this.startedAtMs = System.currentTimeMillis();
            this.sprint = sprint;
            this.lastProgressCheckMs = this.startedAtMs;
        }
    }

    /**
     * State tracking for mining operations.
     */
    private static class MiningState {
        public final BlockPos pos;
        public final long startTick;
        public final Direction direction;
        public float progress;
        public boolean completed;

        public MiningState(BlockPos pos, long startTick, Direction direction) {
            this.pos = pos;
            this.startTick = startTick;
            this.direction = direction;
            this.progress = 0.0f;
            this.completed = false;
        }
    }
}

/*
 * ═══════════════════════════════════════════════════════════════════════════
 * BRIDGESERVER - INTEGRATION & USAGE GUIDE
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * INTEGRATION:
 * ------------
 * 1. Initialize in mod setup (FMLClientSetupEvent):
 *    BridgeServer.start(25575, "optional_token");
 *
 * 2. Process action queue every client tick (ClientTickEvent.Post):
 *    @SubscribeEvent
 *    public void onClientTick(ClientTickEvent.Post event) {
 *        BridgeServer server = BridgeServer.getInstance();
 *        if (server != null && server.isRunning()) {
 *            server.processActionQueue();
 *        }
 *    }
 *
 * 3. Shutdown on mod unload:
 *    BridgeServer.stop();
 *
 * COMMAND EXAMPLES:
 * -----------------
 * Connect to 127.0.0.1:25575 and send JSON commands:
 *
 * {"action": "ping"}
 * {"action": "move", "forward": 1.0, "strafe": 0.0}
 * {"action": "look", "yaw": 90.0, "pitch": -30.0}
 * {"action": "mine", "x": 100, "y": 64, "z": 200}
 * {"action": "place", "x": 101, "y": 64, "z": 200, "facing": "UP"}
 * {"action": "attack"}
 * {"action": "select_slot", "slot": 0}
 * {"action": "get_full_state"}
 * {"action": "scan_blocks", "radius": 16, "block_type": "diamond_ore"}
 * {"action": "harvest", "resource": "wood", "radius": 24}
 *
 * SNAPSHOT BROADCAST:
 * -------------------
 * Server automatically broadcasts complete game state every 1 second (configurable)
 * to all connected clients. Contains player stats, inventory, environment, entities.
 *
 * COMPILE & RUN:
 * --------------
 * 1. Ensure a supported Minecraft Forge SDK is configured
 * 2. Place this file in the mod's bridge package.
 * 3. Build: ./gradlew build
 * 4. Run client, server starts automatically if integrated in mod events
 *
 * TROUBLESHOOTING:
 * ----------------
 * - Port 25575 already in use: Change port in start() call
 * - Connection refused: Ensure server.isRunning() returns true
 * - Actions not executing: Verify processActionQueue() is called in tick handler
 * - Rate limit errors: Reduce command frequency or increase config.commandRateLimit
 *
 * API DOCUMENTATION:
 * ------------------
 * 75+ Commands Implemented:
 *
 * MOVEMENT (10):
 *   move, look, jump, sneak, sprint, swim, stop_moving, crouch, turn
 *
 * COMBAT (9):
 *   attack, use, block, target_entity, get_target, flee, auto_attack, stop_attack
 *
 * INVENTORY (8):
 *   select_slot, swap_slots, drop_item, equip_armor, find_item, sort_inventory, stack_items
 *
 * WORLD INTERACTION (10):
 *   mine, place, get_block_info, check_tool_validity, get_mining_progress, stop_mining,
 *   scan_blocks, find_nearest_block, get_biome, get_light_level, raycast
 *
 * ENTITY INTERACTION (6):
 *   interact_entity, ride_entity, dismount, trade_with_villager, feed_entity, tame_entity
 *
 * NAVIGATION (4):
 *   mark_location, get_waypoints, delete_waypoint, navigate_to
 *
 * STATE QUERIES (6):
 *   get_full_state, get_player_state, get_inventory, get_environment,
 *   get_nearby_entities, get_nearby_blocks
 *
 * SYSTEM (8):
 *   ping, get_metrics, get_snapshot, get_version, get_config, get_server_uptime, stop_server
 *
 * AI INTEGRATION (1):
 *   harvest (player-like scan, navigate, face, tool-select, and mine)
 *
 * TOTAL: 75+ commands fully implemented
 *
 * CAPABILITY SUMMARY:
 * -------------------
 * - Generic vanilla recipe queries and GUI-backed crafting execution
 * - Container inspection, slot clicks, close-container, and crafting-table open support
 * - Movement, navigation, mining, placement, combat, entity interaction, and harvesting
 * - Advancement/status queries, visual summaries, and survival/threat/combat assessment
 * - Request IDs, standardized error envelopes, rate limiting, metrics, and action history
 * - Singleplayer world opening for live validation after title-screen readiness
 */
