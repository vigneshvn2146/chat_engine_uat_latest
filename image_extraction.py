import fitz  # PyMuPDF
import io
import os
from PIL import Image

def extract_real_images_from_pdf(pdf_path, output_folder, skip_fullpage=True, size_threshold=0.95):
    """
    Extract only embedded (non-full-page) images from a PDF.
    Ignores scanned full-page images unless explicitly asked.
    
    Args:
        pdf_path (str): Path to PDF file.
        output_folder (str): Folder to save extracted images.
        skip_fullpage (bool): Whether to skip full-page scans.
        size_threshold (float): % of page area to consider "full-page".
    """
    os.makedirs(output_folder, exist_ok=True)
    pdf = fitz.open(pdf_path)
    print(f"📘 Opened {pdf_path}, total pages: {len(pdf)}")

    image_count = 0
    total_skipped = 0

    for page_index, page in enumerate(pdf, start=1):
        page_images = page.get_images(full=True)
        if not page_images:
            continue

        print(f"\n🧩 Page {page_index}: Found {len(page_images)} image(s)")

        page_rect = page.rect
        page_area = page_rect.width * page_rect.height

        for image_index, img in enumerate(page_images, start=1):
            xref = img[0]
            base_image = pdf.extract_image(xref)
            img_bytes = base_image["image"]
            img_ext = base_image["ext"]

            # Try to detect image position on page (not always available)
            bbox = None
            try:
                for block in page.get_text("rawdict")["blocks"]:
                    if "image" in block:
                        bbox = block["bbox"]
            except Exception:
                pass

            # Estimate if image fills the page
            if bbox:
                (x0, y0, x1, y1) = bbox
                image_area = (x1 - x0) * (y1 - y0)
                ratio = image_area / page_area
                if skip_fullpage and ratio >= size_threshold:
                    total_skipped += 1
                    print(f"⚠️ Skipping full-page image (covers {ratio:.0%} of page)")
                    continue

            image = Image.open(io.BytesIO(img_bytes))
            image_filename = f"page{page_index}_img{image_index}.{img_ext}"
            image_path = os.path.join(output_folder, image_filename)
            image.save(image_path)
            image_count += 1
            print(f"✅ Saved: {image_filename}")

    pdf.close()
    print(f"\n🎉 Done! Extracted {image_count} useful images, skipped {total_skipped} full-page scans.")
    return image_count


# Example usage
pdf_path = r"C:\Users\vignesh.vn\Downloads\Prod Docs\S60ME-C.pdf"
output_folder = r"C:\Users\vignesh.vn\Downloads\images"

extract_real_images_from_pdf(pdf_path, output_folder)



# # AWS Rekognition
# import fitz  # PyMuPDF
# import boto3
# from PIL import Image
# import io
# import os
# from config import config

# def extract_diagrams_with_rekognition(pdf_path, output_folder, dpi=200, min_confidence=60):
#     """
#     Extracts diagrams (like engines, machines, etc.) from each page of a PDF
#     using AWS Rekognition object detection.
    
#     Steps:
#         1. Render PDF pages as images
#         2. Send to Rekognition DetectLabels
#         3. Crop bounding boxes for machine/diagram-like labels
#         4. Save locally as images
#     """

#     os.makedirs(output_folder, exist_ok=True)

#     aws_creds = config.get_aws_credentials()

#     # Initialize AWS Rekognition client
#     client = boto3.client(
#         "rekognition",
#         region_name=aws_creds["region_name"],
#         aws_access_key_id=aws_creds["aws_access_key_id"],
#         aws_secret_access_key=aws_creds["aws_secret_access_key"]
#     )

#     # Open the PDF
#     doc = fitz.open(pdf_path)
#     print(f"📘 Loaded {pdf_path} ({len(doc)} pages)")

#     image_count = 0

#     for page_index, page in enumerate(doc, start=1):
#         # Render each page as an image
#         pix = page.get_pixmap(dpi=dpi)
#         page_img_path = os.path.join(output_folder, f"page_{page_index}.jpg")
#         pix.save(page_img_path)

#         print(f"\n🧭 Processing page {page_index}... ({page_img_path})")

#         # Read image bytes for Rekognition
#         with open(page_img_path, "rb") as f:
#             img_bytes = f.read()

#         # Call Rekognition
#         response = client.detect_labels(
#             Image={"Bytes": img_bytes},
#             MaxLabels=15,
#             MinConfidence=min_confidence
#         )

#         # Load image into PIL for cropping
#         image = Image.open(io.BytesIO(img_bytes))
#         img_width, img_height = image.size

#         diagram_detected = False

#         # Iterate through detected labels
#         for label in response["Labels"]:
#             label_name = label["Name"].lower()

#             # Choose labels relevant to technical diagrams
#             if any(keyword in label_name for keyword in [
#                 "engine", "machine", "mechanical", "motor", "diagram", "machinery", "vehicle"
#             ]):
#                 for i, instance in enumerate(label.get("Instances", [])):
#                     if "BoundingBox" not in instance:
#                         continue
#                     box = instance["BoundingBox"]
#                     left = int(box["Left"] * img_width)
#                     top = int(box["Top"] * img_height)
#                     width = int(box["Width"] * img_width)
#                     height = int(box["Height"] * img_height)

#                     # Crop diagram region
#                     cropped = image.crop((left, top, left + width, top + height))
#                     out_path = os.path.join(output_folder, f"page{page_index}_diagram{i+1}.jpg")
#                     cropped.save(out_path)
#                     print(f"✅ Saved diagram crop: {out_path} (Label: {label_name})")
#                     image_count += 1
#                     diagram_detected = True

#         if not diagram_detected:
#             print("⚠️ No diagram-like object found — skipping.")

#     print(f"\n🎉 Extraction complete. {image_count} diagrams saved to {output_folder}.")
#     return image_count


# # === Example Usage ===
# pdf_path = r"C:\Users\vignesh.vn\Downloads\YANMAR -6EY-18LW-17.pdf"
# output_folder = r"C:\Users\vignesh.vn\Downloads\images"

# extract_diagrams_with_rekognition(pdf_path, output_folder)

