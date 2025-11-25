# ---------------------------------------------------------------------
# OCR Extraction using Tesseract and PaddleOCR
# ---------------------------------------------------------------------

import pytesseract
import fitz  # PyMuPDF
from PIL import Image
import io
import logging
from paddleocr import PaddleOCR
import numpy as np
import cv2

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Specify Tesseract path (adjust if needed)
pytesseract.pytesseract.tesseract_cmd = r"C:\Users\vignesh.vn\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------------------------------------------------------------
# 1️⃣ TESSERACT OCR
# ---------------------------------------------------------------------

def extract_text_tesseract(pdf_path):
    """
    Extracts text from scanned PDFs using Tesseract OCR.
    Handles multi-page PDFs automatically.
    """
    logging.info("🔍 Running Tesseract OCR...")
    pdf = fitz.open(pdf_path)
    all_text = []

    for page_num in range(len(pdf)):
        page = pdf.load_page(page_num)
        pix = page.get_pixmap(dpi=300)
        img = Image.open(io.BytesIO(pix.tobytes("png")))

        # Run OCR
        text = pytesseract.image_to_string(img, lang="eng")
        all_text.append(text.strip())
        logging.info(f"📄 [Tesseract] Page {page_num+1}: Extracted {len(text.split())} words")

    pdf.close()
    combined_text = "\n".join(all_text)
    logging.info(f"✅ Tesseract OCR complete. Total pages processed: {len(all_text)}")
    return combined_text


# ---------------------------------------------------------------------
# 2️⃣ PADDLE OCR
# ---------------------------------------------------------------------

def extract_text_paddle(pdf_path):
    """
    Extracts text from scanned PDFs using PaddleOCR (corrected).
    Detects and recognizes text from full pages.
    """
    logging.info("🌊 Running PaddleOCR...")

    # Modern init: PaddleOCR 3.x compatible
    ocr = PaddleOCR(lang='en', use_textline_orientation=True)

    pdf = fitz.open(pdf_path)
    all_text = []

    for page_num in range(len(pdf)):
        page = pdf.load_page(page_num)
        pix = page.get_pixmap(dpi=300)

        # ✅ Convert PyMuPDF pixmap → OpenCV image (NumPy array)
        img_bytes = pix.tobytes("png")
        nparr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # ✅ Now run PaddleOCR properly
        result = ocr.ocr(img)

        page_text = []
        if result and len(result[0]) > 0:
            for line in result[0]:
                text_line = line[1][0]
                page_text.append(text_line)

        combined_page_text = "\n".join(page_text)
        all_text.append(combined_page_text)
        logging.info(f"📄 [PaddleOCR] Page {page_num+1}: Extracted {len(page_text)} lines")

    pdf.close()
    combined_text = "\n".join(all_text)
    logging.info(f"✅ PaddleOCR complete. Total pages processed: {len(all_text)}")
    return combined_text

# ---------------------------------------------------------------------
# 3️⃣ MAIN EXECUTION
# ---------------------------------------------------------------------

if __name__ == "__main__":
    PDF_FILE = "YANMAR -6N330L- EV-11.pdf"  # Your scanned PDF

    # Run Tesseract
    tesseract_text = extract_text_tesseract(PDF_FILE)
    with open("output_tesseract_ocr.txt", "w", encoding="utf-8") as f:
        f.write(tesseract_text)
    logging.info("📝 Saved: output_tesseract_ocr.txt")

    # Run PaddleOCR
    paddle_text = extract_text_paddle(PDF_FILE)
    with open("output_paddle_ocr.txt", "w", encoding="utf-8") as f:
        f.write(paddle_text)
    logging.info("📝 Saved: output_paddle_ocr.txt")

    logging.info("🏁 OCR extraction complete for both engines.")
