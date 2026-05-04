package com.minecraftbridge;

import net.minecraftforge.common.ForgeConfigSpec;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.event.config.ModConfigEvent;

@Mod.EventBusSubscriber(modid = ExampleMod.MODID, bus = Mod.EventBusSubscriber.Bus.MOD)
public class Config {
    private static final ForgeConfigSpec.Builder BUILDER = new ForgeConfigSpec.Builder();

    private static final ForgeConfigSpec.IntValue BRIDGE_PORT = BUILDER
            .comment("Local TCP port for the Minecraft Bridge JSON API.")
            .defineInRange("bridgePort", 25575, 1024, 65535);

    private static final ForgeConfigSpec.ConfigValue<String> AUTH_TOKEN = BUILDER
            .comment("Optional auth token. Leave empty to disable bridge authentication.")
            .define("authToken", "");

    private static final ForgeConfigSpec.BooleanValue FORCE_UNPAUSE_ON_START = BUILDER
            .comment("Whether AI control should keep the integrated world ticking while pause screens are open.")
            .define("forceUnpauseOnStart", false);

    static final ForgeConfigSpec SPEC = BUILDER.build();

    public static int bridgePort = 25575;
    public static String authToken = "";
    public static boolean forceUnpauseOnStart = false;

    @SubscribeEvent
    static void onLoad(final ModConfigEvent event) {
        bridgePort = BRIDGE_PORT.get();
        authToken = AUTH_TOKEN.get().trim();
        forceUnpauseOnStart = FORCE_UNPAUSE_ON_START.get();
    }

    public static String authTokenOrNull() {
        return authToken == null || authToken.isBlank() ? null : authToken;
    }
}
