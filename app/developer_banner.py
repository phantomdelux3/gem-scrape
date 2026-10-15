"""
Developer banner shown above the tabs: "Developer: <name>" with the name in animated gold -
a shine sweeping across it, a soft glow, a thin outline and twinkling four-pointed sparkles.

Pure Tkinter (no image libraries). Tk has no alpha, so "transparent" colours are blended
with the banner background by hand. The animation only runs while the banner is visible.
"""

import math
import random
import time
import tkinter as tk
from tkinter import font as tkfont

HEIGHT = 38
FRAME_MS = 33                  # ~30 fps
SHINE_SECONDS = 2.6            # one sweep of the shine across the name
BG_TOP, BG_BOTTOM = "#1F2937", "#111827"
CAPTION_COLOR = "#D1D5DB"
GOLD_DARK, GOLD, GOLD_PALE = "#B8860B", "#FFD700", "#FFF8DC"
GOLD_STOPS = [(0.0, GOLD_DARK), (0.2, GOLD), (0.42, GOLD), (0.5, GOLD_PALE), (0.58, GOLD), (0.8, GOLD),
              (1.0, GOLD_DARK)]
GLOW_ALPHA, OUTLINE_ALPHA = 70, 160
SPARKLES = 12
SPARKLE_RADIUS = (3.0, 7.0)
SPARKLE_PERIOD = (0.9, 2.3)


def _rgb(c):
    return int(c[1:3], 16), int(c[3:5], 16), int(c[5:7], 16)


def _mix(a, b, t):
    """Colour a drawn over b with opacity t (0..1)."""
    (r1, g1, b1), (r2, g2, b2) = _rgb(a), _rgb(b)
    return "#%02x%02x%02x" % (round(r2 + (r1 - r2) * t), round(g2 + (g1 - g2) * t), round(b2 + (b1 - b2) * t))


def _gradient(u):
    for (p0, c0), (p1, c1) in zip(GOLD_STOPS, GOLD_STOPS[1:]):
        if u <= p1:
            return _mix(c1, c0, (u - p0) / (p1 - p0) if p1 > p0 else 0)
    return GOLD_STOPS[-1][1]


class DeveloperBanner(tk.Canvas):
    def __init__(self, parent, name, caption="Developer:"):
        super().__init__(parent, height=HEIGHT, highlightthickness=0, borderwidth=0, background=BG_BOTTOM)
        self.name = name
        self.caption = caption
        self.caption_font = tkfont.Font(family="Segoe UI", size=10)
        self.name_font = tkfont.Font(family="Segoe UI", size=12, weight="bold")
        self._chars = []          # (canvas id, centre as a fraction of the name width)
        self._sparkles = []
        self._region = (0, 0, 1, 1)
        self._started = time.time()
        self._job = None
        self.bind("<Configure>", lambda e: self._layout())
        self.bind("<Destroy>", self._on_destroy)
        self._job = self.after(FRAME_MS, self._tick)

    def _bg_at(self, y):
        return _mix(BG_BOTTOM, BG_TOP, min(1.0, max(0.0, y / HEIGHT)))

    # ----- static drawing (on resize) -----------------------------------------
    def _layout(self):
        self.delete("all")
        w = max(self.winfo_width(), 1)
        for y in range(HEIGHT):  # vertical background gradient
            self.create_line(0, y, w, y, fill=self._bg_at(y))

        gap = 6
        cap_w = self.caption_font.measure(self.caption)
        name_w = max(self.name_font.measure(self.name), 1)
        x0 = (w - (cap_w + gap + name_w)) / 2
        cy = HEIGHT / 2
        self.create_text(x0, cy, text=self.caption, anchor="w", font=self.caption_font, fill=CAPTION_COLOR)

        nx = x0 + cap_w + gap
        bg = self._bg_at(cy)
        # glow (4 px wide stroke) and thin outline: copies of the name around its position
        glow = _mix(GOLD, bg, GLOW_ALPHA / 255)
        outline = _mix(GOLD_DARK, bg, OUTLINE_ALPHA / 255)
        for radius, color in ((2, glow), (1, outline)):
            for k in range(8):
                a = k * math.pi / 4
                self.create_text(nx + radius * math.cos(a), cy + radius * math.sin(a), text=self.name, anchor="w",
                                 font=self.name_font, fill=color)
        # the name itself, one item per letter so each can take its own colour from the moving gradient
        self._chars = []
        for i, ch in enumerate(self.name):
            left = self.name_font.measure(self.name[:i])
            mid = (left + self.name_font.measure(ch) / 2) / name_w
            self._chars.append((self.create_text(nx + left, cy, text=ch, anchor="w", font=self.name_font,
                                                 fill=GOLD), mid))

        h = self.name_font.metrics("linespace")
        self._region = (nx - 0.6 * h, cy - h / 2 - 0.25 * h, nx + name_w + 0.6 * h, cy + h / 2 + 0.25 * h)
        self._sparkles = []
        now = time.time()
        for _ in range(SPARKLES):
            s = {"id": self.create_polygon(0, 0, 0, 0, fill=GOLD_PALE, outline="")}
            self._respawn(s, now - random.uniform(0, SPARKLE_PERIOD[1]))  # start at different points
            self._sparkles.append(s)
        self._draw(now)

    def _respawn(self, s, now):
        x1, y1, x2, y2 = self._region
        r = random.uniform(*SPARKLE_RADIUS)
        y = min(max(random.uniform(y1, y2), r), HEIGHT - r)  # keep the whole star inside the banner
        s.update(x=random.uniform(x1, x2), y=y, r=r, period=random.uniform(*SPARKLE_PERIOD), born=now)

    # ----- animation ------------------------------------------------------------
    def _draw(self, now):
        phase = ((now - self._started) / SHINE_SECONDS) % 1.0
        for item, mid in self._chars:
            self.itemconfigure(item, fill=_gradient((mid - phase + 0.5) % 1.0))
        for s in self._sparkles:
            f = (now - s["born"]) / s["period"]
            if f >= 1:
                self._respawn(s, now)
                f = 0.0
            alpha = math.sin(math.pi * f)          # fade in, then out
            r = s["r"] * (0.4 + 0.6 * alpha)
            inner = max(r * 0.3, 1.2)  # waist of the star; never thinner than ~1px so it reads as a star
            pts = []
            for k in range(8):                      # four-pointed star
                a = k * math.pi / 4 - math.pi / 2
                rad = r if k % 2 == 0 else inner
                pts += [s["x"] + rad * math.cos(a), s["y"] + rad * math.sin(a)]
            self.coords(s["id"], *pts)
            self.itemconfigure(s["id"], fill=_mix(GOLD_PALE, self._bg_at(s["y"]), alpha))

    def _tick(self):
        self._job = None
        try:
            visible = self.winfo_viewable() and self.winfo_toplevel().state() != "iconic"
        except tk.TclError:
            return
        if visible and self._chars:
            self._draw(time.time())
        # hidden or minimised: check back slowly instead of animating
        self._job = self.after(FRAME_MS if visible else 500, self._tick)

    def _on_destroy(self, event):
        if event.widget is self and self._job:
            self.after_cancel(self._job)
            self._job = None
