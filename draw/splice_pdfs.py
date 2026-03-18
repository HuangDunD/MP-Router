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
    
    # ======= 微调参数 (可在下方修改) =======
    # 控制缩放比例
    scale_img1 = 1.0  
    scale_img2 = 1.0  # 如果图2偏大，可改成 0.95 等
    # 控制上下平移：正数表示向上移动，负数表示向下移动
    y_shift_img1 = -25
    y_shift_img2 = 0
    # =======================================

    # 等比例缩放高度较小的图片，使其高度与较高的图片一致
    target_height = max(img1.height, img2.height)
    resample_filter = getattr(Image, 'Resampling', Image).LANCZOS
    
    if img1.height < target_height:
        new_width = int(img1.width * (target_height / img1.height) * scale_img1)
        new_height = int(target_height * scale_img1)
        img1 = img1.resize((new_width, new_height), resample_filter)
    else:
        img1 = img1.resize((int(img1.width * scale_img1), int(img1.height * scale_img1)), resample_filter)
        
    if img2.height < target_height:
        new_width = int(img2.width * (target_height / img2.height) * scale_img2)
        new_height = int(target_height * scale_img2)
        img2 = img2.resize((new_width, new_height), resample_filter)
    else:
        img2 = img2.resize((int(img2.width * scale_img2), int(img2.height * scale_img2)), resample_filter)
    
    # Calculate new dimensions
    padding = int(0.1 * dpi) # Small padding
    total_width = img1.width + img2.width + padding
    # 动态计算高度，去掉多余的白边
    max_height = max(img1.height + max(0, y_shift_img1), img2.height + max(0, y_shift_img2))
    
    # Create new blank image (white background)
    combined_img = Image.new('RGB', (total_width, max_height), (255, 255, 255))
    
    # Paste images
    # Align bottom, then apply shift
    y1_offset = max_height - img1.height - y_shift_img1
    combined_img.paste(img1, (0, y1_offset))
    
    y2_offset = max_height - img2.height - y_shift_img2
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
