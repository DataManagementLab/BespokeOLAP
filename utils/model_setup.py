import os

from openai import AsyncOpenAI


def setup_model_config(model_arg: str) -> tuple[bool, str, str | None, AsyncOpenAI]:
    model_name = model_arg
    litellm_prefix = "litellm/"
    use_litellm = model_name.startswith(litellm_prefix)
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    if use_litellm:
        model_name = model_name[len(litellm_prefix) :]
        api_key = (
            os.environ.get("LITELLM_API_KEY")
            or os.environ.get("ANTHROPIC_API_KEY")
            or os.environ.get("OPENAI_API_KEY")
        )
        if not api_key:
            raise RuntimeError(
                "LITELLM_API_KEY (or provider API key) must be set for litellm models."
            )
    else:
        api_key = openai_api_key
    client = AsyncOpenAI(api_key=openai_api_key)
    return use_litellm, model_name, api_key, client
