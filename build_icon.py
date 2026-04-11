"""Run once to regenerate icon.ico"""
from PIL import Image, ImageDraw
import math

def draw_star(draw, cx, cy, r_outer, r_inner, points, color):
    coords = []
    for i in range(points * 2):
        angle = math.pi / points * i - math.pi / 2
        r = r_outer if i % 2 == 0 else r_inner
        coords.append((cx + r * math.cos(angle), cy + r * math.sin(angle)))
    draw.polygon(coords, fill=color)

SIZE = 256

img = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
draw = ImageDraw.Draw(img)

# Deep space background circle
draw.ellipse([0, 0, SIZE - 1, SIZE - 1], fill=(12, 12, 35, 255))

# Faint small stars
small_stars = [(60, 45), (190, 70), (40, 160), (210, 130), (150, 210), (80, 210), (220, 190)]
for x, y in small_stars:
    draw.ellipse([x-2, y-2, x+2, y+2], fill=(200, 210, 255, 180))

# Main bright star (4-point diffraction spike style)
cx, cy = SIZE // 2, SIZE // 2

# Glow
for radius, alpha in [(48, 30), (36, 60), (24, 100)]:
    draw.ellipse([cx-radius, cy-radius, cx+radius, cy+radius], fill=(255, 240, 180, alpha))

# Core
draw.ellipse([cx-10, cy-10, cx+10, cy+10], fill=(255, 255, 220, 255))

# Diffraction spikes
spike_len = 72
spike_width = 3
for angle in [0, 90, 180, 270]:
    rad = math.radians(angle)
    ex = cx + spike_len * math.cos(rad)
    ey = cy + spike_len * math.sin(rad)
    draw.line([cx, cy, ex, ey], fill=(255, 255, 200, 180), width=spike_width)

# Diagonal spikes (shorter)
for angle in [45, 135, 225, 315]:
    rad = math.radians(angle)
    ex = cx + 36 * math.cos(rad)
    ey = cy + 36 * math.sin(rad)
    draw.line([cx, cy, ex, ey], fill=(255, 255, 200, 100), width=2)

sizes = [256, 128, 64, 48, 32, 16]
icons = [img.resize((s, s), Image.LANCZOS) for s in sizes]
icons[0].save("icon.ico", format="ICO", sizes=[(s, s) for s in sizes], append_images=icons[1:])
print("icon.ico written")
