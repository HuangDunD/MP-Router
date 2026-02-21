#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Splice PDFs side-by-side by converting to high-res images first.
Requires: pdftoppm (poppler-utils)
"""

import os
import subprocess
from PIL import Image

def splice_pdfs(pdf1_path, pdf2_path, output_path, dpi=600):
    # Temp file names
    temp_prefix1 = "temp_pdf1"
    temp_prefix2 = "temp_pdf2"
    
    # 1. Convert PDFs to PNGs using pdftoppm
    # -r sets DPI, -png sets format
    print(f"Converting {pdf1_path} to image...")
    subprocess.run(["pdftoppm", "-png", "-r", str(dpi), "-singlefile", pdf1_path, temp_prefix1], check=True)
    
    print(f"Converting {pdf2_path} to image...")
    subprocess.run(["pdftoppm", "-png", "-r", str(dpi), "-singlefile", pdf2_path, temp_prefix2], check=True)
    
    img1_path = f"{temp_prefix1}.png"
    img2_path = f"{temp_prefix2}.png"
    
    if not os.path.exists(img1_path) or not os.path.exists(img2_path):
        print("Error: Image conversion failed.")
        return

    # 2. Combine images using Pillow
    img1 = Image.open(img1_path)
    img2 = Image.open(img2_path)
    
    # Calculate new dimensions
    # Total width = w1 + w2 + padding?
    # Max height = max(h1, h2)
    # We might want to align them. Usually align top or center.
    # Let's align center vertically.
    
    padding = int(0.1 * dpi) # Small padding
    total_width = img1.width + img2.width + padding
    max_height = max(img1.height, img2.height)
    
    # Create new blank image (white background)
    combined_img = Image.new('RGB', (total_width, max_height), (255, 255, 255))
    
    # Paste images
    # Align bottom: offset = max_height - image.height
    
    # Left image (img1)
    y1_offset = max_height - img1.height
    combined_img.paste(img1, (0, y1_offset))
    
    # Right of img1 for img2
    y2_offset = max_height - img2.height
    combined_img.paste(img2, (img1.width + padding, y2_offset))
    
    # 3. Save as PDF
    print(f"Saving combined PDF to {output_path}...")
    combined_img.save(output_path, "PDF", resolution=dpi)
    
    # Cleanup
    os.remove(img1_path)
    os.remove(img2_path)
    print("Done.")

if __name__ == "__main__":
    pdf1 = "figs/ablation_bar.pdf"
    pdf2 = "figs/time_breakdown_horiz.pdf"
    output = "figs/combined_ablation_time_spliced.pdf"
    
    if os.path.exists(pdf1) and os.path.exists(pdf2):
        splice_pdfs(pdf1, pdf2, output)
    else:
        print(f"Error: Input files not found: {pdf1}, {pdf2}")
