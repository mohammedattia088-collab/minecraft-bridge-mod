package com.minecraftbridge;

import com.mojang.logging.LogUtils;
import org.slf4j.Logger;

import net.minecraft.client.Minecraft;
import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.common.MinecraftForge;
import net.minecraftforge.event.server.ServerStartingEvent;
import net.minecraftforge.eventbus.api.IEventBus;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.config.ModConfig;
import net.minecraftforge.fml.event.lifecycle.FMLClientSetupEvent;
import net.minecraftforge.fml.event.lifecycle.FMLCommonSetupEvent;
import net.minecraftforge.fml.javafmlmod.FMLJavaModLoadingContext;

@Mod(ExampleMod.MODID)
public class ExampleMod {
    public static final String MODID = "minecraftbridge";
    private static final Logger LOGGER = LogUtils.getLogger();

    public ExampleMod(FMLJavaModLoadingContext context) {
        IEventBus modEventBus = context.getModEventBus();

        // lifecycle listeners
        modEventBus.addListener(this::commonSetup);

        // general event bus
        MinecraftForge.EVENT_BUS.register(this);

        // register config through the constructor-provided Forge context
        context.registerConfig(ModConfig.Type.COMMON, Config.SPEC);
    }

    private void commonSetup(final FMLCommonSetupEvent event) {
        LOGGER.info("Minecraft Bridge common setup complete");
    }

    @SubscribeEvent
    public void onServerStarting(ServerStartingEvent event) {
        LOGGER.info("Minecraft Bridge observed server starting");
    }

    @Mod.EventBusSubscriber(modid = MODID, bus = Mod.EventBusSubscriber.Bus.MOD, value = Dist.CLIENT)
    public static class ClientModEvents {
        @SubscribeEvent
        public static void onClientSetup(FMLClientSetupEvent event) {
            LOGGER.info("Minecraft Bridge client setup complete");
            event.enqueueWork(() -> {
                try {
                    BridgeServer.start(Config.bridgePort, Config.authTokenOrNull());
                    LOGGER.info("BridgeServer started successfully on port {}.", Config.bridgePort);
                } catch (Exception e) {
                    LOGGER.error("Failed to start BridgeServer", e);
                }
                if (Config.forceUnpauseOnStart) {
                    UnpauseHandler.enableForceUnpause();
                }
                LOGGER.info("MINECRAFT NAME >> {}", Minecraft.getInstance().getUser().getName());
            });
        }
    }
}
