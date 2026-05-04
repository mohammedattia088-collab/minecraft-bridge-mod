# Minecraft Bridge Development Plan

## Naming And Packaging Standard

- Mod id: `minecraftbridge`
- Java package: `com.minecraftbridge`
- Artifact name: `minecraftbridge`
- Public API namespace: JSON action names use lowercase snake case.
- Runtime version reporting must come from the loaded Minecraft/Forge/mod metadata, not fixed Java strings.

## Version Agnostic Java Strategy

1. Keep Minecraft-version-specific calls behind small adapter methods.
2. Prefer stable registries, holders, tags, and metadata APIs over direct version constants.
3. Use reflection fallbacks only for narrow API drift, such as food components across mappings.
4. Keep Gradle properties as the selected build target; true multi-version support should use source sets or separate branches per Minecraft/Forge mapping line.
5. Add a compile matrix before claiming support beyond the configured Forge target.

## Player Replication Roadmap

1. Observation layer: combine bridge JSON state, inventory, nearby entities, nearby blocks, raycasts, and optional vision frames into one timestamped observation.
2. Action layer: model real player controls as tick-duration key states, mouse deltas, hotbar selection, use/attack holds, inventory clicks, and crafting commands.
3. Synchronization: every command should return an action id, requested tick, accepted tick, completion state, and final observation hash.
4. Safety: hard limits for reach distance, scan radius, action queue size, health, lava/fire/cactus checks, and disconnect cleanup.
5. Skills: implement reusable player skills for movement, mining, placing, eating, combat, inventory sorting, crafting, sheltering, and recovery.
6. Learning: start with scripted demonstrations and behavior cloning, then add RL fine tuning once the observation/action contract is stable.
7. Evaluation: replay fixed scenarios and score survival, progress, block interaction accuracy, inventory correctness, and human-like timing.

## Test Plan

- Java: `./gradlew clean build` on every change.
- Protocol smoke: connect to `127.0.0.1:25575`, verify `ping`, `get_version`, `get_full_state`, `move`, `look`, `stop_moving`, and `harvest`.
- Python: syntax check all project files, then unit-smoke logger/scheduler/perception without launching Minecraft.
- Integration: launch client run config, enter a world, connect Python bridge client, and execute a short recorded action script.
- Regression: store JSON traces for successful episodes and compare key fields across builds.

## Implemented Foundation

- Java bridge request ids for reliable Python request/response matching.
- Java AI control commands: `get_ai_status`, `pause_harvest`, `resume_harvest`, `stop_harvest`, `set_unpause`.
- Python bridge client: `/Users/moabualazm/PycharmProjects/ZoomNameAutomator/A2CSPython/OtherStuff/MinecraftAI/bridge/client.py`
- Python basic mechanics: look, wander, sprint, jump, scan logs, mine logs, collect items, flee, attack, place practice, harvest trees.
- Python autonomous learner: tabular Q-learning policy with persisted policy and JSONL experience log.
- Runner: `/Users/moabualazm/PycharmProjects/ZoomNameAutomator/A2CSPython/OtherStuff/MinecraftAI/run_autonomous_player.py`

Run after launching Minecraft with the rebuilt mod jar and entering a world:

```bash
python3 /Users/moabualazm/PycharmProjects/ZoomNameAutomator/A2CSPython/OtherStuff/MinecraftAI/run_autonomous_player.py --forever
```

For a bounded smoke run:

```bash
python3 /Users/moabualazm/PycharmProjects/ZoomNameAutomator/A2CSPython/OtherStuff/MinecraftAI/run_autonomous_player.py --steps 100
```

## Milestones

1. Stable bridge foundation: compile-clean mod, no duplicate sources, correct mod id, client-only bridge start.
2. Reliable Python client: reconnecting TCP client with request/response correlation and timeout handling.
3. Human control primitives: tick-held movement, smooth look, use/attack holds, block targeting, and inventory clicks.
4. Task skills: mine a visible block, collect dropped item, craft planks/tools, eat when hungry, flee hostile mobs.
5. Autonomous loop: observe, choose skill/action, execute, verify, recover.
6. Training loop: collect demonstrations, train policy, evaluate in fixed worlds, then expand to randomized worlds.
