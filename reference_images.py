"""
Bruynzeel reference image library.
Maps MCP option keys + style descriptors → real Bruynzeel kitchen photo URLs.
Images are full-resolution (800KB–1MB) from bruynzeelkeukens.nl.
"""

CDN = "https://www.bruynzeelkeukens.nl/sites/default/files/keukenopstelling/"

# model option_key (feature 1) → best matching kitchen photo
MODEL_IMAGES: dict[str, str] = {
    "L9":  "Karakter Natuur Eiken Cam 1.jpg",          # Laren — warm oak closest match
    "L11": "Karakter Natuur Eiken Cam 1.jpg",           # Laren Stroken
    "A11": "Atlas middengrijs 1.jpg",                   # Atlas Kaderdeur
    "A13": "Andes Wit i.c.m. Atlas Leisteen bw_1.jpg",  # Andes
    "A18": "Atlas Trend Cubaniet Hoofdbeeld.jpg",        # Atlas Trend
    "A21": "Circo-Atlas-winkel_recht 2.jpg",             # Circo Atlas
    "A8":  "Atlas middengrijs 1.jpg",                   # Atlas Laser
    "E5":  "Atlas-Kalkwit-1.jpg",                       # Excellent → white
    "M10": "Atlas middengrijs 1.jpg",                   # Matterhorn → grey
    "N10": "018 Bolton wit nieuwe rastermaat cam 1-min_0.jpg",  # Newport
    "N6":  "Aerdenhout licht eiken 2.jpg",              # Aerdenhout
    "O1":  "Olympia kristalgrijs origineel_0.jpg",      # Olympia
    "O6":  "Olympia mosgroen - 19 WEB.jpg",             # Olympia Stroken
    "P4":  "Atlas-Kalkwit-1.jpg",                       # Pallas Laser
    "R10": "Dudok Kalkwit - 1.jpg",                     # Romantiek → country
    "T2":  "Atlas middengrijs 1.jpg",                   # Thema 3000
}

# front color/finish option_key (feature 101) → style modifier
COLOR_IMAGES: dict[str, str] = {
    # Warm wood tones
    "AE2": "Aerdenhout licht eiken 2.jpg",       # kastanje eiken
    "BG8": "Holten koper eiken 11 LQ.jpg",       # beigegrijs eiken
    "BR8": "Holten koper eiken 11 LQ.jpg",       # brons eiken
    "CC6": "BK-Naarden-Grijs walnoot Hoofdbeeld.jpg",  # cacao eiken
    "EB":  "Aerdenhout licht eiken 2.jpg",       # lak gebeitst fineer eiken
    # Whites / light
    "WI12": "Atlas-Kalkwit-1.jpg",               # wit
    "KW4":  "Atlas-Kalkwit-1.jpg",               # kalkwit
    "BW6":  "Atlas-Kalkwit-1.jpg",               # betonwit
    # Greys
    "WJ1":  "Atlas middengrijs 1.jpg",           # warm grijs
    "WJ2":  "Atlas middengrijs 1.jpg",           # warm grijs verticaal
    # Dark
    "ZW":   "Atlas zwart staal cam 2-min.jpg",   # zwart
    # Green
    "MG":   "Olympia mosgroen - 19 WEB.jpg",     # mosgroen
    "SG":   "BK-Atlas-Zilvergroen- 020623-71 bw_0.jpg",  # zilvergroen
    # Blue
    "SB":   "Olympia Staalblauw Hoofdbeeld.jpg", # staalblauw
}

# style tag → fallback image
STYLE_IMAGES: dict[str, str] = {
    "modern":        "Atlas middengrijs 1.jpg",
    "klassiek":      "Dudok Kalkwit - 1.jpg",
    "landelijk":     "Brighton-Coco-33_0.jpg",
    "industrieel":   "Atlas zwart staal cam 2-min.jpg",
    "scandinavisch": "Atlas-Kalkwit-1.jpg",
    "warm":          "Aerdenhout licht eiken 2.jpg",
    "donker":        "Atlas zwart staal cam 2-min.jpg",
    "wit":           "Atlas-Kalkwit-1.jpg",
    "eiken":         "Aerdenhout licht eiken 2.jpg",
    "hout":          "Holten koper eiken 11 LQ.jpg",
    "grijs":         "Atlas middengrijs 1.jpg",
    "groen":         "Olympia mosgroen - 19 WEB.jpg",
    "blauw":         "Olympia Staalblauw Hoofdbeeld.jpg",
    "eiland":        "Circo-Atlas-winkel_recht 2.jpg",
    "marmer":        "Jura zandsteen 1 LQ.jpg",
}

DEFAULT_IMAGE = "Atlas middengrijs 1.jpg"


def pick_reference_image(
    model_key: str = "",
    color_key: str = "",
    style_tags: list[str] = None,
) -> str:
    """
    Return the CDN URL of the best matching reference kitchen photo.
    Priority: model_key > color_key > style_tag > default
    """
    if model_key and model_key in MODEL_IMAGES:
        return CDN + MODEL_IMAGES[model_key]
    if color_key and color_key in COLOR_IMAGES:
        return CDN + COLOR_IMAGES[color_key]
    for tag in (style_tags or []):
        tl = tag.lower()
        for key, img in STYLE_IMAGES.items():
            if key in tl:
                return CDN + img
    return CDN + DEFAULT_IMAGE


def all_image_urls() -> list[str]:
    """All available reference image URLs."""
    seen = set()
    urls = []
    for d in [MODEL_IMAGES, COLOR_IMAGES, STYLE_IMAGES]:
        for fname in d.values():
            if fname not in seen:
                seen.add(fname)
                urls.append(CDN + fname)
    return urls
