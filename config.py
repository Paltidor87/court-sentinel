import os

# Runtime Tier (cost, elite, custom)
BOT_TIER = os.getenv("BOT_TIER", "cost").strip().lower()

# Model Presets
# Priority Order:
# 1. Local Ollama (Free - 0 tokens)
# 2. Direct Google Gemini (Plan Tokens)
# 3. OpenRouter (Billable Credits)
TIER_PRESETS = {
    "cost": {
        "chat_model": "llama3:latest", # Local Ollama
        "chat_fallbacks": "qwen3:8b,gemini-1.5-flash,openai/gpt-4o-mini",
        "vision_model": "gemini-1.5-flash", # No local vision yet, using Plan
        "vision_fallbacks": "openai/gpt-4o-mini,nvidia/nemotron-nano-12b-v2-vl:free",
        "ugc_model": "gemini-1.5-flash",
        "ugc_fallbacks": "qwen3:8b,openai/gpt-4o-mini",
    },
    "elite": {
        "chat_model": "gemini-1.5-pro", # Plan Tokens
        "chat_fallbacks": "openai/gpt-4o,openai/gpt-4o-mini",
        "vision_model": "gemini-1.5-pro",
        "vision_fallbacks": "openai/gpt-4o,openai/gpt-4o-mini",
        "ugc_model": "gemini-1.5-pro",
        "ugc_fallbacks": "openai/gpt-4o,openai/gpt-4o-mini",
    },
}

def resolve_setting(setting_name: str, env_var_name: str, fallback_default: str) -> str:
    """Resolve a setting based on the current BOT_TIER or environment override."""
    if BOT_TIER == "custom":
        return os.getenv(env_var_name, fallback_default)
    tier = TIER_PRESETS.get(BOT_TIER, TIER_PRESETS["cost"])
    return tier.get(setting_name, fallback_default)
