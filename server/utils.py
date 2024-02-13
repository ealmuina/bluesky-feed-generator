import re


def remove_emoji(text):
    """Removes all emojis from a string using regular expressions."""
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U0001f900-\U0001f9ff"
                               u"\U0001f300-\U0001f5ff"
                               u"\U0001f600-\U0001f64f"
                               u"\U0001f680-\U0001f6ff"
                               u"\U0001f1e0-\U0001f1ff"
                               u"\U00002702-\U000027b0"
                               u"\U0001f900-\U0001f9ff"
                               u"\U0001f300-\U0001f5ff"
                               u"\U0001f600-\U0001f64f"
                               u"\U0001f680-\U0001f6ff"
                               u"\U0001f1e0-\U0001f1ff"
                               u"\U00002702-\U000027b0"
                               u"\U0001f900-\U0001f9ff"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r"", text)


def remove_links(text):
    url_pattern = r"\S+\.\S+"
    cleaned_text = []

    for word in text.split():
        if re.match(url_pattern, word) or word.startswith("@"):
            continue
        cleaned_text.append(word)

    return " ".join(cleaned_text)
