import os
import io
import qrcode
import logging
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

def generate_upi_card(upi_id: str, amount: str, story_name: str, payee_name: str = "Merchant", output_path: str = "upi_card.png"):
    """
    Refined V2 Template Generator:
    Strictly follows Poppins-style rules and removes all { } placeholders.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    assets_dir = os.path.join(base_dir, "assets")
    base_path = os.path.join(assets_dir, "upi_template_v2.png")
    
    if not os.path.exists(base_path):
        logger.error(f"Template not found at {base_path}")
        return None

    img = Image.open(base_path).convert("RGBA")
    draw = ImageDraw.Draw(img)
    
    # 1. CLEAN PHACEHOLDERS (Wipe solid white)
    # Story Name
    draw.rectangle([100, 250, 582, 300], fill="white")
    # Amount
    draw.rectangle([100, 310, 582, 390], fill="white")
    # QR Interior (Leave the pill 'Scan or pay' alone)
    draw.rectangle([250, 440, 435, 755], fill="white")
    # UPI ID (Wipe entire line area)
    draw.rectangle([100, 770, 582, 830], fill="white")
    
    # 2. FONTS
    try:
        import platform
        if platform.system() == "Windows":
            font_path = "C:/Windows/Fonts/arial.ttf"
            font_bold_path = "C:/Windows/Fonts/ariblk.ttf"
        else:
            # Common path for ttf-mscorefonts-installer on Ubuntu/Debian
            font_path = "/usr/share/fonts/truetype/msttcorefonts/Arial.ttf"
            font_bold_path = "/usr/share/fonts/truetype/msttcorefonts/Arial_Black.ttf"
            if not os.path.exists(font_bold_path):
                font_bold_path = "/usr/share/fonts/truetype/msttcorefonts/arialbd.ttf"
                
        f_story = ImageFont.truetype(font_path, 34)
        f_amount = ImageFont.truetype(font_bold_path, 64)
        f_upi = ImageFont.truetype(font_path, 24)
    except Exception as e:
        logger.error(f"Failed to load fonts: {e}")
        f_story = f_amount = f_upi = ImageFont.load_default()

    cx = img.width // 2
    
    # 3. OVERLAY DYNAMIC TEXT
    # Story Name (#333333, Regular) - EXACT CASE SENSITIVE
    draw.text((cx, 275), story_name, fill=(51, 51, 51), font=f_story, anchor="mm")
    
    # Amount (#000000, Bold)
    draw.text((cx, 350), f"₹{amount}", fill=(0, 0, 0), font=f_amount, anchor="mm")
    
    # UPI ID (#666666, Regular)
    total_upi_txt = f"UPI ID: {upi_id}"
    draw.text((cx, 792), total_upi_txt, fill=(102, 102, 102), font=f_upi, anchor="mm")

    # 4. OVERLAY QR
    # Minimal payload to bypass banking risk heuristics
    upi_payload = f"upi://pay?pa={upi_id}&pn={payee_name}&am={amount}&cu=INR"
    qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_H, box_size=8, border=1)
    qr.add_data(upi_payload)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGBA")
    
    qr_size = 310
    qr_img = qr_img.resize((qr_size, qr_size), Image.Resampling.LANCZOS)
    # Paste centered horizontally, and aligned with the box
    img.paste(qr_img, (cx - qr_size // 2, 438), qr_img)

    # Return BytesIO
    img_buffer = io.BytesIO()
    img.convert("RGB").save(img_buffer, "JPEG", quality=95)
    img_buffer.seek(0)
    return img_buffer
