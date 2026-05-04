# Minecraft Version Compatibility Matrix

Generated: 2026-05-03

This checkout is a ForgeGradle client-side bridge compiled against official Mojang mappings. Compatibility is tracked per concrete Minecraft plus loader artifact in `compatibility/matrix.tsv`; property profiles live in `compatibility/*.properties`.

## Verified Profiles

| Profile | Loader | Minecraft | Loader Artifact | Status | Check |
| --- | --- | --- | --- | --- | --- |
| `local-1.21.1` | Forge | `1.21.1` | `52.1.5` | Verified | `./scripts/check_version_matrix.sh` |
| `forge-1.21.1` | Forge | `1.21.1` | `52.1.14` | Verified | `CHECK_LATEST=1 ./scripts/check_version_matrix.sh` |

## Failed Forge Profiles

The full run was executed with:

```bash
CHECK_ALL=1 ./scripts/check_version_matrix.sh
```

Concrete Forge profiles were added for every available Forge 1.21.x artifact reported by Forge promotions: `1.21`, `1.21.1`, `1.21.3`, `1.21.4`, `1.21.5`, `1.21.6`, `1.21.7`, `1.21.8`, `1.21.9`, `1.21.10`, and `1.21.11`.

Only `1.21.1` passes with the current source. All other Forge lines fail `compileJava` and need compatibility source branches:

| Profile | Result | Main Compile Blocker |
| --- | --- | --- |
| `forge-1.21` | Fail | `FMLJavaModLoadingContext.registerConfig` API mismatch |
| `forge-1.21.3` | Fail | level height APIs, recipe APIs, `Ingredient`, `RecipeBookMenu` changes |
| `forge-1.21.4` | Fail | same recipe/level API drift as `1.21.3` |
| `forge-1.21.5` | Fail | private inventory fields, client input, movement helper, recipe API drift |
| `forge-1.21.6` through `forge-1.21.10` | Fail | Forge eventbus package/API plus Minecraft client/recipe/inventory API drift |
| `forge-1.21.11` | Fail | `ResourceLocation`/`Villager` package changes plus Forge eventbus and recipe API drift |

## NeoForge Profiles

Concrete NeoForge profiles were added for `1.21` through `1.21.11`, including beta-only lines where NeoForge publishes only beta artifacts. The checker records them as `branch-required` because this checkout uses ForgeGradle and `net.minecraftforge` imports. They are intentionally not claimed as build failures of NeoForge itself.

## Branch Policy

Create `compat/forge-<minecraft-version>` or `compat/neoforge-<minecraft-version>` only when source changes are required. In this workspace no Git branch was created because `/Volumes/Local SSD/RandomDump/Downloads/minecraft-bridge-mod` is not a Git repository; the matrix records the branch names that should be created in the real VCS checkout.

Before marking a row verified:

1. Add or update `compatibility/<profile>.properties`.
2. Run `PROFILE=<profile> ./scripts/check_version_matrix.sh` or `CHECK_ALL=1 ./scripts/check_version_matrix.sh`.
3. Launch the client once and verify bridge startup plus `get_version`, `get_full_state`, `list_recipes`, `verify_crafting_recipes`, `craft`, `get_container`, `get_advancements`, and `get_visual_summary`.
4. Record the result in `compatibility/matrix.tsv`.

Sources checked: Forge promotions JSON (`https://files.minecraftforge.net/net/minecraftforge/forge/promotions_slim.json`) and NeoForge Maven index (`https://maven.neoforged.net/releases/net/neoforged/neoforge/`).
