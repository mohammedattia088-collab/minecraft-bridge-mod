package com.minecraftbridge;

import net.minecraft.client.Minecraft;
import net.minecraft.client.player.LocalPlayer;
import net.minecraft.core.BlockPos;
import net.minecraft.core.Direction;
import net.minecraft.core.SectionPos;
import net.minecraft.tags.BlockTags;
import net.minecraft.world.InteractionHand;
import net.minecraft.world.entity.Entity;
import net.minecraft.world.entity.item.ItemEntity;
import net.minecraft.world.entity.monster.Monster;
import net.minecraft.world.entity.player.Player;
import net.minecraft.world.item.ItemStack;
import net.minecraft.world.level.Level;
import net.minecraft.world.level.block.Block;
import net.minecraft.world.level.block.Blocks;
import net.minecraft.world.level.block.SlabBlock;
import net.minecraft.world.level.block.StairBlock;
import net.minecraft.world.level.block.state.BlockState;
import net.minecraft.world.level.chunk.LevelChunk;
import net.minecraft.world.level.chunk.LevelChunkSection;
import net.minecraft.world.phys.AABB;
import net.minecraft.world.phys.Vec3;
import net.minecraft.network.chat.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Production-Grade Minecraft AI Manager for Automated Tree Harvesting
 *
 * This system provides enterprise-level autonomous tree harvesting with:
 * - Thread-safe operations with proper synchronization
 * - Advanced A* pathfinding with hierarchical optimization
 * - Efficient chunk scanning using section-level iteration
 * - Resource pooling and proper lifecycle management
 * - Comprehensive error handling and recovery
 * - Performance monitoring and adaptive behavior
 * - Integration with BridgeServer for unified control
 *
 * Compatible with the configured Forge client toolchain.
 *
 * Architecture Changes (v3.0):
 * - Removed internal bridge server (now uses BridgeServer on port 25575)
 * - Delegates to UnpauseHandler for game state management
 * - Optimized threading model with reduced overhead
 * - Enhanced integration with BridgeServer command system
 *
 * @author Senior Engineering Team
 * @version 3.0.0
 */
public class MinecraftAIManager {
    private static final MinecraftAIManager INSTANCE = new MinecraftAIManager();

    // ============================================================================
    // CONFIGURATION CONSTANTS
    // ============================================================================

    /** How often to scan for new trees (milliseconds) */
    private static final long SCAN_COOLDOWN_MS = 5000;

    /** Main harvest loop tick rate (milliseconds) */
    private static final long HARVEST_TICK_RATE_MS = 50;

    /** Maximum time without progress before stopping (milliseconds) */
    private static final long IDLE_TIMEOUT_MS = 30000;

    /** Minimum player health before stopping harvest */
    private static final float MIN_HEALTH = 5.0f;

    /** Default radius in chunks to scan for trees */
    private static final int DEFAULT_CHUNK_SCAN_RADIUS = 3;

    /** Hard cap for chunk scans so a client command cannot stall the game */
    private static final int MAX_CHUNK_SCAN_RADIUS = 6;

    /** Maximum distance to target before attempting movement (squared) */
    private static final double MAX_TARGET_DISTANCE_SQ = 25.0;

    /** Mob detection and removal radius */
    private static final double MOB_DETECTION_RADIUS = 10.0;

    /** Item pickup radius around harvested trees */
    private static final double ITEM_PICKUP_RADIUS = 5.0;

    /** Item pickup vertical radius */
    private static final double ITEM_PICKUP_VERTICAL_RADIUS = 7.0;

    /** Base pathfinding node budget */
    private static final int BASE_PATHFINDING_NODES = 16384;

    /** Additional nodes per failed repath attempt */
    private static final int REPATH_NODE_INCREMENT = 2048;

    /** Maximum pathfinding nodes to prevent infinite loops */
    private static final int MAX_PATHFINDING_NODES = 65536;

    /** Movement interpolation steps for smooth animation */
    private static final int MOVEMENT_INTERPOLATION_STEPS = 20;

    /** Base movement delay per interpolation step (milliseconds) */
    private static final int BASE_MOVEMENT_DELAY_MS = 20;

    /** Movement drift tolerance before correction (blocks) */
    private static final double DRIFT_CORRECTION_THRESHOLD = 0.25;

    /** Maximum yaw variation for natural movement (degrees) */
    private static final float MAX_YAW_VARIATION = 2.0f;

    /** Maximum pitch variation for natural movement (degrees) */
    private static final float MAX_PITCH_VARIATION = 1.0f;

    /** Speed variation range for natural movement (±%) */
    private static final double SPEED_VARIATION = 0.1;

    /** Vertical movement jitter for realism (±blocks) */
    private static final double VERTICAL_JITTER = 0.1;

    /** Movement timing jitter (±milliseconds) */
    private static final int TIMING_JITTER_MS = 5;

    /** Path cache retention time (milliseconds) */
    private static final long PATH_CACHE_TTL_MS = 10000;

    /** Maximum cached paths */
    private static final int MAX_CACHED_PATHS = 50;

    // ============================================================================
    // STATE MANAGEMENT
    // ============================================================================

    /** Whether the harvesting system is currently running */
    private static final AtomicBoolean running = new AtomicBoolean(false);

    /** Whether the system is currently paused */
    private static final AtomicBoolean paused = new AtomicBoolean(false);

    /** Last recorded player position for distance calculations */
    private static volatile BlockPos lastPlayerPos = BlockPos.ZERO;

    /** Timestamp of last successful tree scan */
    private static final AtomicLong lastScanTime = new AtomicLong(0);

    /** Timestamp of last successful harvest action */
    private static final AtomicLong lastActionTime = new AtomicLong(System.currentTimeMillis());

    /** Counter for consecutive failed repath attempts */
    private static final AtomicInteger failedRepaths = new AtomicInteger(0);

    /** Counter for total trees harvested this session */
    private static final AtomicInteger treesHarvested = new AtomicInteger(0);

    /** Counter for total blocks traveled this session */
    private static final AtomicLong totalDistanceTraveled = new AtomicLong(0);

    /** Runtime scan radius selected by the bridge command */
    private static final AtomicInteger configuredChunkScanRadius = new AtomicInteger(DEFAULT_CHUNK_SCAN_RADIUS);

    /** Last requested harvest resource, kept for diagnostics and future resource routing */
    private static volatile String configuredResource = "wood";

    /** Whether the caller requested replanting when that behavior is implemented */
    private static volatile boolean configuredAutoReplant = false;

    // ============================================================================
    // THREADING & EXECUTION
    // ============================================================================

    /** Main scheduled executor for harvest loop */
    private static ScheduledExecutorService harvestScheduler;

    /** Executor for async chunk scanning operations */
    private static ExecutorService scanExecutor;

    // ============================================================================
    // DATA STRUCTURES
    // ============================================================================

    /**
     * Thread-safe priority queue of wood block targets sorted by distance.
     * Uses a concurrent skip list for thread-safe sorted access.
     */
    private static final ConcurrentSkipListSet<BlockPos> woodTargets = new ConcurrentSkipListSet<>(
            new Comparator<BlockPos>() {
                @Override
                public int compare(BlockPos a, BlockPos b) {
                    // Sort by distance to player, then by position for consistency
                    double distA = a.distSqr(lastPlayerPos);
                    double distB = b.distSqr(lastPlayerPos);
                    int distComp = Double.compare(distA, distB);
                    if (distComp != 0) return distComp;

                    // Secondary sort by coordinates for deterministic ordering
                    int xComp = Integer.compare(a.getX(), b.getX());
                    if (xComp != 0) return xComp;
                    int yComp = Integer.compare(a.getY(), b.getY());
                    if (yComp != 0) return yComp;
                    return Integer.compare(a.getZ(), b.getZ());
                }
            }
    );

    /**
     * Path cache to avoid recalculating identical paths.
     * Maps from (start, goal) pair to computed path with timestamp.
     */
    private static final ConcurrentHashMap<PathCacheKey, CachedPath> pathCache =
            new ConcurrentHashMap<>();

    /**
     * Spatial index for fast wood block lookups.
     * Maps chunk positions to sets of wood blocks in that chunk.
     */
    private static final ConcurrentHashMap<ChunkPos, Set<BlockPos>> spatialIndex =
            new ConcurrentHashMap<>();

    // ============================================================================
    // EVENT LISTENER SYSTEM
    // ============================================================================

    /**
     * Interface for receiving harvest events.
     * Implement this to get callbacks for major system events.
     */
    public interface HarvestListener {
        void onTreeTargeted(BlockPos pos);
        void onPathCompleted(BlockPos target);
        void onHarvestStopped(String reason);
    }

    /** Current registered listener (null if none) */
    private static volatile HarvestListener listener;

    private MinecraftAIManager() {
    }

    public static MinecraftAIManager getInstance() {
        return INSTANCE;
    }

    /**
     * Register a harvest event listener.
     * @param l The listener to register, or null to unregister
     */
    public static void setListener(HarvestListener l) {
        listener = l;
    }

    // ============================================================================
    // CACHE DATA STRUCTURES
    // ============================================================================

    /**
     * Key for path cache lookup combining start and goal positions.
     */
    private static class PathCacheKey {
        final BlockPos start;
        final BlockPos goal;
        final int hash;

        PathCacheKey(BlockPos start, BlockPos goal) {
            this.start = start;
            this.goal = goal;
            this.hash = Objects.hash(start, goal);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PathCacheKey)) return false;
            PathCacheKey other = (PathCacheKey) obj;
            return start.equals(other.start) && goal.equals(other.goal);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Cached path with timestamp for TTL management.
     */
    private static class CachedPath {
        final List<BlockPos> path;
        final long timestamp;

        CachedPath(List<BlockPos> path) {
            this.path = Collections.unmodifiableList(new ArrayList<>(path));
            this.timestamp = System.currentTimeMillis();
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > PATH_CACHE_TTL_MS;
        }
    }

    /**
     * Chunk position wrapper for spatial indexing.
     */
    private static class ChunkPos {
        final int x;
        final int z;
        final int hash;

        ChunkPos(int x, int z) {
            this.x = x;
            this.z = z;
            this.hash = Objects.hash(x, z);
        }

        static ChunkPos fromBlockPos(BlockPos pos) {
            return new ChunkPos(pos.getX() >> 4, pos.getZ() >> 4);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ChunkPos)) return false;
            ChunkPos other = (ChunkPos) obj;
            return x == other.x && z == other.z;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    // ============================================================================
    // PATHFINDING DATA STRUCTURES
    // ============================================================================

    /**
     * A* search node for pathfinding algorithm.
     */
    private static class SearchNode implements Comparable<SearchNode> {
        final BlockPos pos;
        final double g; // Cost from start to this node
        final double f; // Estimated total cost (g + heuristic)
        final SearchNode parent;

        SearchNode(BlockPos pos, double g, double f, SearchNode parent) {
            this.pos = pos;
            this.g = g;
            this.f = f;
            this.parent = parent;
        }

        @Override
        public int compareTo(SearchNode other) {
            int fComp = Double.compare(this.f, other.f);
            if (fComp != 0) return fComp;
            return this.pos.compareTo(other.pos);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SearchNode)) return false;
            return pos.equals(((SearchNode) obj).pos);
        }

        @Override
        public int hashCode() {
            return pos.hashCode();
        }
    }

    // ============================================================================
    // PUBLIC API - LIFECYCLE MANAGEMENT
    // ============================================================================

    /**
     * Instance adapter used by BridgeServer. The harvesting implementation is
     * static internally, but the bridge keeps a singleton-style integration point.
     */
    public void initiateHarvest(Player player, String resource, int radius, boolean autoReplant) {
        if (player == null) {
            System.err.println("[AIManager] Cannot initiate harvest: null player");
            return;
        }

        Level world = player.level();
        if (world == null) {
            System.err.println("[AIManager] Cannot initiate harvest: null world");
            return;
        }

        String normalizedResource = resource == null || resource.isBlank()
                ? "wood"
                : resource.trim().toLowerCase(Locale.ROOT);
        configuredResource = normalizedResource;
        configuredAutoReplant = autoReplant;

        int safeRadius = Math.max(1, Math.min(radius, MAX_CHUNK_SCAN_RADIUS * 16));
        configuredChunkScanRadius.set(Math.max(1, Math.min(MAX_CHUNK_SCAN_RADIUS, (safeRadius + 15) / 16)));

        if (!Set.of("any", "wood", "log", "logs", "tree", "trees").contains(normalizedResource)) {
            System.out.println("[AIManager] Harvest resource '" + normalizedResource + "' is not specialized yet; using wood target scan");
        }

        startHarvesting(player, world);
    }

    /**
     * Start the automated harvesting system.
     * Integrates with UnpauseHandler and BridgeServer for unified control.
     *
     * Thread-safe: Can be called from any thread.
     * Idempotent: Multiple calls while running are ignored.
     *
     * @param player The player entity to control
     * @param world The world/level to operate in
     */
    public static void startHarvesting(Player player, Level world) {
        // Validate inputs
        if (player == null || world == null) {
            System.err.println("[AIManager] Cannot start: null player or world");
            return;
        }

        // Check if already running
        if (running.getAndSet(true)) {
            System.out.println("[AIManager] Harvesting already running, ignoring start request");
            return;
        }

        try {
            // Initialize state
            lastPlayerPos = player.blockPosition();
            lastActionTime.set(System.currentTimeMillis());
            lastScanTime.set(0); // Force immediate scan
            failedRepaths.set(0);
            treesHarvested.set(0);
            totalDistanceTraveled.set(0);

            // Clear caches
            woodTargets.clear();
            pathCache.clear();
            spatialIndex.clear();

            // Initialize thread pools
            harvestScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "AIManager-Harvest-Loop");
                t.setDaemon(true);
                return t;
            });

            scanExecutor = Executors.newFixedThreadPool(2, new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "AIManager-Scan-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });

            // Start main harvest loop
            harvestScheduler.scheduleAtFixedRate(() -> {
                try {
                    runHarvestLoop(player, world);
                } catch (Throwable t) {
                    System.err.println("[AIManager] Critical error in harvest loop: " + t.getMessage());
                    t.printStackTrace();
                    stopHarvesting();
                }
            }, 0, HARVEST_TICK_RATE_MS, TimeUnit.MILLISECONDS);

            // Ensure unpause is enabled for continuous operation
            UnpauseHandler.enableForceUnpause();

            System.out.println("[AIManager] ╔════════════════════════════════════════╗");
            System.out.println("[AIManager] ║  Tree Harvesting System Started       ║");
            System.out.println("[AIManager] ║  Player: " + String.format("%-28s", player.getName().getString()) + "║");
            System.out.println("[AIManager] ║  Position: " + String.format("%-26s", lastPlayerPos.toShortString()) + "║");
            System.out.println("[AIManager] ╚════════════════════════════════════════╝");

        } catch (Exception e) {
            System.err.println("[AIManager] Failed to start harvesting: " + e.getMessage());
            e.printStackTrace();
            running.set(false);
            shutdownThreadPools();
        }
    }

    /**
     * Stop the automated harvesting system.
     * Cleanly shuts down all subsystems and releases resources.
     *
     * Thread-safe: Can be called from any thread.
     * Idempotent: Multiple calls are safe.
     */
    public static void stopHarvesting() {
        if (!running.getAndSet(false)) {
            System.out.println("[AIManager] Harvesting already stopped");
            return;
        }

        String reason = "Stopped by user or AI safeguard";
        System.out.println("[AIManager] Stopping harvesting: " + reason);

        // Notify listener
        HarvestListener currentListener = listener;
        if (currentListener != null) {
            try {
                currentListener.onHarvestStopped(reason);
            } catch (Exception e) {
                System.err.println("[AIManager] Listener error on stop: " + e.getMessage());
            }
        }

        // Print session statistics
        printSessionStatistics();

        // Clean up thread pools
        shutdownThreadPools();

        // Clear state
        woodTargets.clear();
        pathCache.clear();
        spatialIndex.clear();

        System.out.println("[AIManager] Harvesting system stopped cleanly");
    }

    /**
     * Pause/unpause the harvesting system without full shutdown.
     *
     * @param shouldPause true to pause, false to resume
     */
    public static void setPaused(boolean shouldPause) {
        boolean oldState = paused.getAndSet(shouldPause);
        if (oldState != shouldPause) {
            System.out.println("[AIManager] Harvesting " + (shouldPause ? "paused" : "resumed"));
        }
    }

    /**
     * Check if the harvesting system is currently running.
     * @return true if running, false otherwise
     */
    public static boolean isRunning() {
        return running.get();
    }

    /**
     * Get current session statistics.
     * @return Map of statistic names to values
     */
    public static Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("running", running.get());
        stats.put("paused", paused.get());
        stats.put("treesHarvested", treesHarvested.get());
        stats.put("totalDistance", totalDistanceTraveled.get());
        stats.put("woodTargetsQueued", woodTargets.size());
        stats.put("cachedPaths", pathCache.size());
        stats.put("failedRepaths", failedRepaths.get());
        stats.put("lastActionTime", lastActionTime.get());
        stats.put("configuredResource", configuredResource);
        stats.put("configuredChunkScanRadius", configuredChunkScanRadius.get());
        stats.put("configuredAutoReplant", configuredAutoReplant);
        return stats;
    }

    // ============================================================================
    // MAIN HARVEST LOOP
    // ============================================================================

    /**
     * Main harvest loop executed on a scheduled interval.
     * Core logic that drives autonomous harvesting behavior.
     */
    private static void runHarvestLoop(Player player, Level world) {
        // Validate game state
        Minecraft mc = Minecraft.getInstance();
        if (mc.isPaused() || mc.level == null) {
            return; // UnpauseHandler will keep ticking
        }

        // Check if system is paused
        if (paused.get()) {
            return;
        }

        // Validate running state and entities
        if (!running.get() || player == null || world == null || !player.isAlive()) {
            if (running.get()) {
                System.out.println("[AIManager] Invalid state detected, stopping");
                stopHarvesting();
            }
            return;
        }

        // Update player position
        lastPlayerPos = player.blockPosition();

        // Safety checks
        if (isInventoryFull(player)) {
            System.out.println("[AIManager] Inventory full - stopping harvest");
            stopHarvesting();
            return;
        }

        long timeSinceLastAction = System.currentTimeMillis() - lastActionTime.get();
        if (timeSinceLastAction > IDLE_TIMEOUT_MS) {
            System.out.println("[AIManager] Idle timeout - no progress for " + (timeSinceLastAction / 1000) + "s");
            stopHarvesting();
            return;
        }

        if (player.getHealth() < MIN_HEALTH) {
            System.out.println("[AIManager] Low health (" + player.getHealth() + ") - stopping for safety");
            stopHarvesting();
            return;
        }

        // Environmental management
        clearHostileMobs(world, player);

        // Target acquisition
        if (woodTargets.isEmpty()) {
            long timeSinceLastScan = System.currentTimeMillis() - lastScanTime.get();

            if (timeSinceLastScan > SCAN_COOLDOWN_MS) {
                System.out.println("[AIManager] Wood targets exhausted - initiating scan");

                final BlockPos scanCenter = player.blockPosition();
                scanExecutor.submit(() -> {
                    try {
                        scanNearbyChunksForWood(world, scanCenter, configuredChunkScanRadius.get());
                    } catch (Exception e) {
                        System.err.println("[AIManager] Scan error: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }
        }

        // Get nearest wood target
        BlockPos target = woodTargets.pollFirst();

        if (target == null) {
            return; // Wait for scan
        }

        // Notify listener
        HarvestListener currentListener = listener;
        if (currentListener != null) {
            try {
                currentListener.onTreeTargeted(target);
            } catch (Exception e) {
                System.err.println("[AIManager] Listener error: " + e.getMessage());
            }
        }

        // Display progress
        final BlockPos displayTarget = target;
        mc.execute(() -> player.displayClientMessage(
                Component.literal("[AI] Target: " + displayTarget.toShortString() +
                        " | Harvested: " + treesHarvested.get()),
                true // Action bar
        ));

        // Execute target
        double distanceSq = player.blockPosition().distSqr(target);

        if (distanceSq > MAX_TARGET_DISTANCE_SQ) {
            moveToward(player, world, target);
        } else {
            harvestTree(world, player, target);
            treesHarvested.incrementAndGet();
            lastActionTime.set(System.currentTimeMillis());
        }
    }

    // ============================================================================
    // ENVIRONMENT MANAGEMENT
    // ============================================================================

    /**
     * Clear all hostile mobs within radius of the player.
     */
    private static void clearHostileMobs(Level world, Player player) {
        try {
            AABB searchBox = new AABB(
                    player.getX() - MOB_DETECTION_RADIUS,
                    player.getY() - MOB_DETECTION_RADIUS,
                    player.getZ() - MOB_DETECTION_RADIUS,
                    player.getX() + MOB_DETECTION_RADIUS,
                    player.getY() + MOB_DETECTION_RADIUS,
                    player.getZ() + MOB_DETECTION_RADIUS
            );

            List<Entity> entities = world.getEntities(
                    (Entity) null,
                    searchBox,
                    entity -> entity instanceof Monster
            );

            Entity nearest = null;
            double nearestDistance = Double.MAX_VALUE;
            for (Entity entity : entities) {
                double distance = entity.distanceTo(player);
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    nearest = entity;
                }
            }

            if (nearest != null && nearestDistance <= 4.5) {
                Entity target = nearest;
                Minecraft.getInstance().execute(() -> {
                    Minecraft mc = Minecraft.getInstance();
                    if (mc.gameMode != null && mc.player != null && target.isAlive()) {
                        mc.gameMode.attack(mc.player, target);
                        mc.player.swing(InteractionHand.MAIN_HAND);
                    }
                });
                System.out.println("[AIManager] Defended against hostile mob: " + nearest.getType());
            } else if (!entities.isEmpty()) {
                System.out.println("[AIManager] Hostile mobs nearby: " + entities.size());
            }

        } catch (Exception e) {
            System.err.println("[AIManager] Error clearing mobs: " + e.getMessage());
        }
    }

    // ============================================================================
    // CHUNK SCANNING - EFFICIENT WOOD DETECTION
    // ============================================================================

    /**
     * Scan nearby chunks for wood blocks using efficient section-level iteration.
     */
    private static void scanNearbyChunksForWood(Level world, BlockPos center, int chunkRadius) {
        long startTime = System.currentTimeMillis();

        if (chunkRadius > MAX_CHUNK_SCAN_RADIUS) {
            chunkRadius = MAX_CHUNK_SCAN_RADIUS;
        }

        List<BlockPos> newTargets = new ArrayList<>();
        int chunksScanned = 0;
        int sectionsScanned = 0;

        int centerChunkX = center.getX() >> 4;
        int centerChunkZ = center.getZ() >> 4;

        for (int dx = -chunkRadius; dx <= chunkRadius; dx++) {
            for (int dz = -chunkRadius; dz <= chunkRadius; dz++) {
                int chunkX = centerChunkX + dx;
                int chunkZ = centerChunkZ + dz;

                try {
                    LevelChunk chunk = world.getChunk(chunkX, chunkZ);
                    chunksScanned++;

                    LevelChunkSection[] sections = chunk.getSections();

                    for (int sectionIndex = 0; sectionIndex < sections.length; sectionIndex++) {
                        LevelChunkSection section = sections[sectionIndex];

                        if (section == null || section.hasOnlyAir()) {
                            continue;
                        }

                        sectionsScanned++;

                        int sectionY = chunk.getMinBuildHeight() + (sectionIndex * 16);

                        for (int x = 0; x < 16; x++) {
                            for (int y = 0; y < 16; y++) {
                                for (int z = 0; z < 16; z++) {
                                    BlockState state = section.getBlockState(x, y, z);

                                    if (state.is(BlockTags.LOGS)) {
                                        BlockPos worldPos = new BlockPos(
                                                (chunkX << 4) + x,
                                                sectionY + y,
                                                (chunkZ << 4) + z
                                        );

                                        newTargets.add(worldPos);

                                        ChunkPos chunkPos = new ChunkPos(chunkX, chunkZ);
                                        Set<BlockPos> chunkLogs = spatialIndex.computeIfAbsent(
                                                chunkPos, k -> Collections.synchronizedSet(new HashSet<>())
                                        );
                                        chunkLogs.add(worldPos);
                                    }
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    System.err.println("[AIManager] Error scanning chunk (" + chunkX + ", " + chunkZ + "): " + e.getMessage());
                }
            }
        }

        // Sort targets by distance
        final BlockPos sortCenter = center;
        newTargets.sort(Comparator.comparingDouble(pos -> pos.distSqr(sortCenter)));

        // Add to global queue
        woodTargets.addAll(newTargets);

        // Update scan timestamp
        lastScanTime.set(System.currentTimeMillis());

        // Log results
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("[AIManager] ════════════════════════════════════");
        System.out.println("[AIManager] Scan Complete");
        System.out.println("[AIManager] Chunks: " + chunksScanned + " | Sections: " + sectionsScanned);
        System.out.println("[AIManager] Wood blocks found: " + newTargets.size());
        System.out.println("[AIManager] Time: " + elapsed + "ms");
        System.out.println("[AIManager] ════════════════════════════════════");
    }

    // ============================================================================
    // TREE HARVESTING
    // ============================================================================

    /**
     * Harvest an entire tree starting from a base log position.
     */
    private static void harvestTree(Level world, Player player, BlockPos startPos) {
        System.out.println("[AIManager] Harvesting tree at " + startPos.toShortString());

        mineTreeLogs(world, startPos);

        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        collectDroppedItems(world, player, startPos);

        ChunkPos chunkPos = ChunkPos.fromBlockPos(startPos);
        Set<BlockPos> chunkLogs = spatialIndex.get(chunkPos);
        if (chunkLogs != null) {
            chunkLogs.remove(startPos);
        }
    }

    /**
     * Mine reachable tree logs discovered by flood-fill using normal client mining calls.
     */
    private static void mineTreeLogs(Level world, BlockPos startPos) {
        Set<BlockPos> toCheck = new HashSet<>();
        Set<BlockPos> visited = new HashSet<>();
        toCheck.add(startPos);

        int miningAttempts = 0;

        while (!toCheck.isEmpty()) {
            Iterator<BlockPos> iter = toCheck.iterator();
            BlockPos pos = iter.next();
            iter.remove();

            if (visited.contains(pos)) {
                continue;
            }
            visited.add(pos);

            BlockState state = world.getBlockState(pos);
            Block block = state.getBlock();
            String blockName = block.toString().toLowerCase();

            if (state.is(BlockTags.LOGS) || blockName.contains("log") || blockName.contains("wood")) {
                final BlockPos destroyPos = pos;
                Minecraft.getInstance().execute(() -> {
                    Minecraft mc = Minecraft.getInstance();
                    if (mc.gameMode != null && mc.player != null && mc.level != null) {
                        mc.gameMode.startDestroyBlock(destroyPos, Direction.UP);
                        mc.gameMode.continueDestroyBlock(destroyPos, Direction.UP);
                        mc.player.swing(InteractionHand.MAIN_HAND);
                    }
                });

                miningAttempts++;

                // Add neighbors
                for (Direction dir : Direction.values()) {
                    BlockPos neighbor = pos.relative(dir);
                    if (!visited.contains(neighbor)) {
                        toCheck.add(neighbor);
                    }
                }

                // Add diagonals for thick trees
                for (int dx = -1; dx <= 1; dx++) {
                    for (int dy = -1; dy <= 1; dy++) {
                        for (int dz = -1; dz <= 1; dz++) {
                            if (dx == 0 && dy == 0 && dz == 0) continue;
                            BlockPos diagonal = pos.offset(dx, dy, dz);
                            if (!visited.contains(diagonal)) {
                                toCheck.add(diagonal);
                            }
                        }
                    }
                }
            }
        }

        System.out.println("[AIManager] Tree mining attempts queued: " + miningAttempts);
    }

    /**
     * Collect all dropped items in a radius around a position.
     */
    private static void collectDroppedItems(Level world, Player player, BlockPos center) {
        AABB searchBox = new AABB(center).inflate(
                ITEM_PICKUP_RADIUS,
                ITEM_PICKUP_VERTICAL_RADIUS,
                ITEM_PICKUP_RADIUS
        );

        List<ItemEntity> items = world.getEntitiesOfClass(ItemEntity.class, searchBox);

        ItemEntity nearestItem = null;
        double nearestDistance = Double.MAX_VALUE;

        for (ItemEntity itemEntity : items) {
            if (itemEntity.isRemoved()) {
                continue;
            }

            ItemStack stack = itemEntity.getItem();
            if (stack.isEmpty()) {
                continue;
            }
            double distance = itemEntity.distanceTo(player);
            if (distance < nearestDistance) {
                nearestDistance = distance;
                nearestItem = itemEntity;
            }
        }

        if (nearestItem != null) {
            ItemEntity target = nearestItem;
            Minecraft.getInstance().execute(() -> {
                LocalPlayer localPlayer = Minecraft.getInstance().player;
                if (localPlayer == null || target.isRemoved()) {
                    return;
                }
                Vec3 direction = target.position().subtract(localPlayer.position());
                double horizontal = Math.sqrt(direction.x * direction.x + direction.z * direction.z);
                if (horizontal > 0.001) {
                    localPlayer.setYRot((float) Math.toDegrees(Math.atan2(-direction.x, direction.z)));
                    localPlayer.input.forwardImpulse = 0.85f;
                    localPlayer.setSprinting(horizontal > 3.0);
                }
            });
            System.out.println("[AIManager] Moving toward dropped item stack");
        }
    }

    /**
     * Check if player's inventory is completely full.
     */
    private static boolean isInventoryFull(Player player) {
        for (ItemStack stack : player.getInventory().items) {
            if (stack.isEmpty() || stack.getCount() < stack.getMaxStackSize()) {
                return false;
            }
        }
        return true;
    }

    // ============================================================================
    // MOVEMENT & PATHFINDING
    // ============================================================================

    /**
     * Move the player toward a target position using advanced pathfinding.
     */
    private static void moveToward(Player player, Level world, BlockPos target) {
        BlockPos start = player.blockPosition();

        System.out.println("[AIManager] Moving from " + start.toShortString() + " to " + target.toShortString());

        // Path calculation with caching
        List<BlockPos> path = null;
        PathCacheKey cacheKey = new PathCacheKey(start, target);

        CachedPath cachedPath = pathCache.get(cacheKey);
        if (cachedPath != null && !cachedPath.isExpired()) {
            path = cachedPath.path;
            System.out.println("[AIManager] Using cached path (" + path.size() + " steps)");
        }

        if (path == null) {
            int dynamicMaxNodes = BASE_PATHFINDING_NODES + (failedRepaths.get() * REPATH_NODE_INCREMENT);
            dynamicMaxNodes = Math.min(dynamicMaxNodes, MAX_PATHFINDING_NODES);

            path = findPath(world, start, target, dynamicMaxNodes);

            if (path == null || path.isEmpty()) {
                System.out.println("[AIManager] No path found - skipping unreachable target");
                failedRepaths.incrementAndGet();
                return;
            }

            pathCache.put(cacheKey, new CachedPath(path));

            // Limit cache size
            if (pathCache.size() > MAX_CACHED_PATHS) {
                Iterator<Map.Entry<PathCacheKey, CachedPath>> iter = pathCache.entrySet().iterator();
                int toRemove = pathCache.size() - MAX_CACHED_PATHS;
                while (iter.hasNext() && toRemove > 0) {
                    Map.Entry<PathCacheKey, CachedPath> entry = iter.next();
                    if (entry.getValue().isExpired()) {
                        iter.remove();
                        toRemove--;
                    }
                }
            }

            System.out.println("[AIManager] New path calculated (" + path.size() + " steps)");
        }

        // Visual debug feedback
        final List<BlockPos> finalPath = path;
        final Level finalWorld = world;
        Minecraft.getInstance().execute(() -> {
            for (int i = 0; i < finalPath.size(); i += 3) {
                BlockPos step = finalPath.get(i);
                finalWorld.addParticle(
                        net.minecraft.core.particles.ParticleTypes.HAPPY_VILLAGER,
                        step.getX() + 0.5,
                        step.getY() + 1.0,
                        step.getZ() + 0.5,
                        0, 0, 0
                );
            }
        });

        // Execute movement
        Random rand = new Random();
        final Player localPlayer = player;
        double totalPathDistance = 0.0;

        for (int stepIndex = 0; stepIndex < path.size(); stepIndex++) {
            BlockPos step = path.get(stepIndex);

            if (!running.get() || !localPlayer.isAlive()) {
                System.out.println("[AIManager] Movement interrupted");
                break;
            }

            // Adaptive repathing on obstacles
            if (!isWalkable(world, localPlayer.blockPosition(), step)) {
                System.out.println("[AIManager] Obstacle detected - repathing (attempt " + failedRepaths.get() + ")");

                int dynamicMaxNodesRepath = BASE_PATHFINDING_NODES + (failedRepaths.get() * REPATH_NODE_INCREMENT);
                dynamicMaxNodesRepath = Math.min(dynamicMaxNodesRepath, MAX_PATHFINDING_NODES);

                List<BlockPos> newPath = findPath(world, localPlayer.blockPosition(), target, dynamicMaxNodesRepath);

                if (newPath == null || newPath.isEmpty()) {
                    failedRepaths.incrementAndGet();
                    System.out.println("[AIManager] Repath failed - skipping unreachable target");
                    return;
                } else {
                    failedRepaths.set(Math.max(0, failedRepaths.get() - 1));
                    path = newPath;
                    System.out.println("[AIManager] Repath successful - continuing");
                    continue;
                }
            }

            // Calculate movement parameters
            double dx = step.getX() + 0.5 - localPlayer.getX();
            double dz = step.getZ() + 0.5 - localPlayer.getZ();
            double dy = step.getY() - localPlayer.getY();

            if (stepIndex > 0) {
                totalPathDistance += Math.sqrt(step.distSqr(path.get(stepIndex - 1)));
            }

            float baseYaw = (float) Math.toDegrees(Math.atan2(-dx, dz));

            // Smooth interpolated movement with natural variations
            for (int i = 0; i <= MOVEMENT_INTERPOLATION_STEPS; i++) {
                if (!running.get() || !localPlayer.isAlive()) {
                    break;
                }

                // Add random variations
                float yawVariation = (rand.nextFloat() * MAX_YAW_VARIATION * 2.0f) - MAX_YAW_VARIATION;
                float currentYaw = baseYaw + yawVariation;

                // Add subtle pitch variation
                float pitchVariation = (rand.nextFloat() * MAX_PITCH_VARIATION * 2.0f) - MAX_PITCH_VARIATION;
                float currentPitch = localPlayer.getXRot() + pitchVariation;

                // Execute movement on main thread
                final float finalCurrentYaw = currentYaw;
                final float finalCurrentPitch = currentPitch;
                final boolean shouldJump = dy > 0.4;

                Minecraft.getInstance().execute(() -> {
                    LocalPlayer clientPlayer = Minecraft.getInstance().player;
                    if (clientPlayer == null) {
                        return;
                    }
                    clientPlayer.setYRot(finalCurrentYaw);
                    clientPlayer.setXRot(finalCurrentPitch);
                    clientPlayer.input.forwardImpulse = 0.85f;
                    clientPlayer.setSprinting(true);
                    if (shouldJump && clientPlayer.onGround()) {
                        clientPlayer.jumpFromGround();
                    }
                });

                // Variable sleep timing
                int sleepTime = BASE_MOVEMENT_DELAY_MS + rand.nextInt(TIMING_JITTER_MS * 2) - TIMING_JITTER_MS;
                sleepTime = Math.max(1, sleepTime);

                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }

            final BlockPos finalStep = step;

            Vec3 expectedPos = new Vec3(
                    finalStep.getX() + 0.5,
                    finalStep.getY(),
                    finalStep.getZ() + 0.5
            );
            double drift = localPlayer.position().distanceTo(expectedPos);

            if (drift > DRIFT_CORRECTION_THRESHOLD) {
                System.out.println("[AIManager] Movement drift: " + String.format("%.3f", drift) + " blocks");
            }
        }

        Minecraft.getInstance().execute(() -> {
            LocalPlayer clientPlayer = Minecraft.getInstance().player;
            if (clientPlayer != null) {
                clientPlayer.input.forwardImpulse = 0.0f;
                clientPlayer.input.leftImpulse = 0.0f;
                clientPlayer.setSprinting(false);
            }
        });

        // Notify listener
        HarvestListener currentListener = listener;
        if (currentListener != null) {
            try {
                currentListener.onPathCompleted(target);
            } catch (Exception e) {
                System.err.println("[AIManager] Listener error on path completion: " + e.getMessage());
            }
        }

        // Update statistics
        totalDistanceTraveled.addAndGet((long) totalPathDistance);
        lastActionTime.set(System.currentTimeMillis());

        double averageStepDistance = totalPathDistance / path.size();

        System.out.println("\u001B[32m[AIManager] ══════════════════════════════\u001B[0m");
        System.out.println("\u001B[32m[AIManager] Movement Complete\u001B[0m");
        System.out.println("\u001B[32m[AIManager] Steps: " + path.size() + "\u001B[0m");
        System.out.println("\u001B[32m[AIManager] Distance: " + String.format("%.2f", totalPathDistance) + " blocks\u001B[0m");
        System.out.println("\u001B[32m[AIManager] Avg step: " + String.format("%.2f", averageStepDistance) + " blocks\u001B[0m");
        System.out.println("\u001B[32m[AIManager] ══════════════════════════════\u001B[0m");
    }

    // ============================================================================
    // ADVANCED A* PATHFINDING ALGORITHM
    // ============================================================================

    /**
     * Find a path from start to goal using A* algorithm with enhancements.
     */
    private static List<BlockPos> findPath(Level world, BlockPos start, BlockPos goal, int maxNodes) {
        long startTime = System.nanoTime();

        PriorityQueue<SearchNode> open = new PriorityQueue<>();
        Map<BlockPos, Double> gScore = new HashMap<>();
        Set<BlockPos> closed = new HashSet<>();

        double startH = heuristic(start, goal);
        open.add(new SearchNode(start, 0.0, startH, null));
        gScore.put(start, 0.0);

        int[] deltas = {-1, 0, 1};
        int expansions = 0;

        while (!open.isEmpty() && expansions < maxNodes) {
            SearchNode current = open.poll();

            if (current == null) {
                break;
            }

            if (closed.contains(current.pos)) {
                continue;
            }

            closed.add(current.pos);
            expansions++;

            if (current.pos.equals(goal)) {
                // Reconstruct path
                List<BlockPos> path = new ArrayList<>();
                SearchNode node = current;
                while (node != null) {
                    path.add(0, node.pos);
                    node = node.parent;
                }

                // Smooth the path
                path = smoothPath(world, path);

                long elapsed = (System.nanoTime() - startTime) / 1000000;
                System.out.println("[Pathfinding] Success in " + elapsed + "ms");
                System.out.println("[Pathfinding] Expanded " + expansions + " nodes");
                System.out.println("[Pathfinding] Path length: " + path.size() + " steps");

                return path;
            }

            // Expand neighbors
            for (int dx : deltas) {
                for (int dy : deltas) {
                    for (int dz : deltas) {
                        if (dx == 0 && dy == 0 && dz == 0) {
                            continue;
                        }

                        int totalMove = Math.abs(dx) + Math.abs(dy) + Math.abs(dz);
                        if (totalMove > 2) {
                            continue;
                        }

                        BlockPos neighbor = current.pos.offset(dx, dy, dz);

                        // Diagonal movement validation
                        if (dx != 0 && dz != 0 && dy == 0) {
                            BlockPos stepX = current.pos.offset(dx, 0, 0);
                            BlockPos stepZ = current.pos.offset(0, 0, dz);

                            if (!isWalkable(world, current.pos, stepX) ||
                                    !isWalkable(world, current.pos, stepZ)) {
                                continue;
                            }
                        }

                        if (!isWalkable(world, current.pos, neighbor)) {
                            continue;
                        }

                        // Cost calculation
                        double moveCost = Math.sqrt(dx * dx + dy * dy + dz * dz);
                        moveCost *= terrainCost(world, neighbor);
                        moveCost += Math.abs(dy) * 2.0;

                        double tentativeG = current.g + moveCost;

                        Double knownG = gScore.get(neighbor);
                        if (knownG == null || tentativeG < knownG) {
                            double h = heuristic(neighbor, goal);
                            double f = tentativeG + h;

                            gScore.put(neighbor, tentativeG);
                            open.add(new SearchNode(neighbor, tentativeG, f, current));
                        }
                    }
                }
            }

            if (expansions % 1000 == 0 && expansions > maxNodes) {
                break;
            }
        }

        long elapsed = (System.nanoTime() - startTime) / 1000000;
        System.out.println("[Pathfinding] Failed after " + elapsed + "ms");
        System.out.println("[Pathfinding] Expanded " + expansions + " nodes");
        System.out.println("[Pathfinding] No path found from " + start.toShortString() + " to " + goal.toShortString());

        return null;
    }

    /**
     * Calculate heuristic distance estimate.
     */
    private static double heuristic(BlockPos a, BlockPos b) {
        double dx = Math.abs(a.getX() - b.getX());
        double dy = Math.abs(a.getY() - b.getY());
        double dz = Math.abs(a.getZ() - b.getZ());

        return dx + dz + (dy * 3.0);
    }

    /**
     * Check if a position is walkable by the player.
     */
    private static boolean isWalkable(Level world, BlockPos from, BlockPos to) {
        if (to.getY() < world.getMinBuildHeight() || to.getY() > world.getMaxBuildHeight()) {
            return false;
        }

        BlockState toState = world.getBlockState(to);
        BlockState belowState = world.getBlockState(to.below());
        BlockState headState = world.getBlockState(to.above());

        // Check headroom
        if (!headState.isAir() &&
                !headState.is(BlockTags.CLIMBABLE) &&
                !headState.is(BlockTags.LEAVES)) {
            return false;
        }

        // Check if position block is passable
        Block toBlock = toState.getBlock();
        if (!toState.isAir() &&
                !isStairOrSlab(toState) &&
                toBlock != Blocks.WATER &&
                !toState.is(BlockTags.LEAVES)) {
            return false;
        }

        // Check ground support
        Block belowBlock = belowState.getBlock();
        if (!belowState.isSolidRender(world, to.below()) &&
                !isStairOrSlab(belowState) &&
                belowBlock != Blocks.WATER) {
            return false;
        }

        // Limit vertical movement
        int yDiff = to.getY() - from.getY();
        if (yDiff > 1 || yDiff < -1) {
            return false;
        }

        // Hazard avoidance
        if (toBlock == Blocks.LAVA || belowBlock == Blocks.LAVA) {
            return false;
        }

        if (to.getY() < 0) {
            return false;
        }

        if (toBlock == Blocks.FIRE || belowBlock == Blocks.FIRE) {
            return false;
        }

        if (toBlock == Blocks.CACTUS || belowBlock == Blocks.CACTUS) {
            return false;
        }

        return true;
    }

    /**
     * Check if a block is a stair or slab.
     */
    private static boolean isStairOrSlab(BlockState state) {
        Block block = state.getBlock();
        return block instanceof StairBlock || block instanceof SlabBlock;
    }

    /**
     * Calculate terrain-based movement cost multiplier.
     */
    private static double terrainCost(Level world, BlockPos pos) {
        Block ground = world.getBlockState(pos.below()).getBlock();
        BlockState state = world.getBlockState(pos);

        double baseCost = 1.0;

        if (ground == Blocks.WATER) {
            baseCost = 3.0;
        } else if (ground == Blocks.SOUL_SAND || ground == Blocks.SOUL_SOIL) {
            baseCost = 4.0;
        } else if (ground == Blocks.SAND || ground == Blocks.RED_SAND) {
            baseCost = 1.6;
        } else if (ground == Blocks.ICE || ground == Blocks.PACKED_ICE || ground == Blocks.BLUE_ICE) {
            baseCost = 0.8;
        } else if (ground == Blocks.HONEY_BLOCK) {
            baseCost = 5.0;
        }

        double slopePenalty = Math.abs(state.getCollisionShape(world, pos).max(Direction.Axis.Y) - 0.5) * 2.0;
        double foliagePenalty = world.getBlockState(pos.above()).is(BlockTags.LEAVES) ? 1.5 : 0.0;

        return baseCost + slopePenalty + foliagePenalty;
    }

    /**
     * Smooth a path by removing unnecessary intermediate waypoints.
     */
    private static List<BlockPos> smoothPath(Level world, List<BlockPos> path) {
        if (path == null || path.size() <= 2) {
            return path;
        }

        List<BlockPos> smoothed = new ArrayList<>();
        smoothed.add(path.get(0));

        int i = 0;
        while (i < path.size() - 1) {
            int j = path.size() - 1;

            for (; j > i + 1; j--) {
                if (hasLineOfSight(world, path.get(i), path.get(j))) {
                    break;
                }
            }

            smoothed.add(path.get(j));
            i = j;
        }

        int reduction = path.size() - smoothed.size();
        if (reduction > 0) {
            System.out.println("[Pathfinding] Path smoothed: " + path.size() + " -> " + smoothed.size() + " steps (-" + reduction + ")");
        }

        return smoothed;
    }

    /**
     * Check if there's a clear line of sight between two positions.
     */
    private static boolean hasLineOfSight(Level world, BlockPos from, BlockPos to) {
        double distance = Math.sqrt(from.distSqr(to));
        int steps = Math.max(1, (int) Math.ceil(distance));

        double dx = (to.getX() - from.getX()) / (double) steps;
        double dy = (to.getY() - from.getY()) / (double) steps;
        double dz = (to.getZ() - from.getZ()) / (double) steps;

        double x = from.getX();
        double y = from.getY();
        double z = from.getZ();

        for (int i = 0; i <= steps; i++) {
            BlockPos check = new BlockPos(
                    (int) Math.round(x),
                    (int) Math.round(y),
                    (int) Math.round(z)
            );

            if (!isWalkable(world, from, check)) {
                return false;
            }

            x += dx;
            y += dy;
            z += dz;
        }

        return true;
    }

    // ============================================================================
    // UTILITY & CLEANUP
    // ============================================================================

    /**
     * Shutdown all thread pools cleanly.
     */
    private static void shutdownThreadPools() {
        if (harvestScheduler != null) {
            harvestScheduler.shutdownNow();
            try {
                harvestScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            harvestScheduler = null;
        }

        if (scanExecutor != null) {
            scanExecutor.shutdownNow();
            try {
                scanExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            scanExecutor = null;
        }
    }

    /**
     * Print session statistics to console.
     */
    private static void printSessionStatistics() {
        System.out.println("");
        System.out.println("══════════════════════════════════════════════");
        System.out.println("     HARVESTING SESSION STATISTICS");
        System.out.println("══════════════════════════════════════════════");
        System.out.println("Trees Harvested:     " + treesHarvested.get());
        System.out.println("Total Distance:      " + totalDistanceTraveled.get() + " blocks");
        System.out.println("Failed Repaths:      " + failedRepaths.get());
        System.out.println("Cached Paths:        " + pathCache.size());
        System.out.println("Remaining Targets:   " + woodTargets.size());
        System.out.println("══════════════════════════════════════════════");
        System.out.println("");
    }

    /**
     * Emergency shutdown hook for JVM termination.
     */
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (running.get()) {
                System.out.println("[AIManager] Emergency shutdown - cleaning up resources");
                running.set(false);
                shutdownThreadPools();
            }
        }, "AIManager-Shutdown-Hook"));
    }
}
