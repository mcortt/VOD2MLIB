"""Generate logo.png by resizing tools/source_logo.png.

The bundled source (`source_logo.png`) is the canonical artwork for the
plugin. This script resizes it to the target dimensions used by
Dispatcharr's catalogue. Re-run after replacing source_logo.png.

Pixel-art is preserved via NEAREST resampling so chunky pixels stay
crisp when the catalogue scales the logo for display.
"""
from PIL import Image
import os
import sys

TARGET_SIZE = 512  # Square output, side length in pixels

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(HERE, "source_logo.png")
OUT = os.path.join(HERE, "..", "logo.png")


def main() -> int:
    if not os.path.isfile(SOURCE):
        print(f"Missing source: {SOURCE}", file=sys.stderr)
        return 1

    src = Image.open(SOURCE)
    if src.mode != "RGBA":
        src = src.convert("RGBA")

    sw, sh = src.size
    if sw != sh:
        print(f"Warning: source is not square ({sw}x{sh}); output will be stretched.", file=sys.stderr)

    # NEAREST preserves the pixel-art aesthetic on downscale.
    out = src.resize((TARGET_SIZE, TARGET_SIZE), Image.Resampling.NEAREST)
    out_path = os.path.abspath(OUT)
    out.save(out_path, "PNG", optimize=True)
    print(f"Wrote {out_path} ({TARGET_SIZE}x{TARGET_SIZE}) from {SOURCE} ({sw}x{sh})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
