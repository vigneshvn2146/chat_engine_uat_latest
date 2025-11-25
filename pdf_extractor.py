import os, time
from PyPDF2 import PdfReader, PdfWriter
import pikepdf
from concurrent.futures import ProcessPoolExecutor, as_completed

def process_page(input_pdf, output_folder, page_num):
    """Split a single page and compress it"""
    # Read only the required page
    reader = PdfReader(input_pdf)
    writer = PdfWriter()
    writer.add_page(reader.pages[page_num - 1])

    # Save temporary file
    temp_file = os.path.join(output_folder, f"temp_page_{page_num}.pdf")
    with open(temp_file, "wb") as f_out:
        writer.write(f_out)

    # Compress with pikepdf
    compressed_file = os.path.join(output_folder, f"page_{page_num}.pdf")
    with pikepdf.open(temp_file) as pdf:
        pdf.save(
            compressed_file,
            linearize=True,
            recompress_flate=True
        )

    os.remove(temp_file)
    return compressed_file


def split_and_compress_pdf_parallel(input_pdf, output_folder, max_workers=6):
    """Split + compress PDF in parallel"""
    os.makedirs(output_folder, exist_ok=True)

    # Count total pages
    reader = PdfReader(input_pdf)
    total_pages = len(reader.pages)

    compressed_files = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_page, input_pdf, output_folder, i): i
            for i in range(1, total_pages + 1)
        }

        for future in as_completed(futures):
            page_num = futures[future]
            try:
                compressed_file = future.result()
                compressed_files.append(compressed_file)
                print(f"✅ Page {page_num} compressed -> {compressed_file}")
            except Exception as e:
                print(f"❌ Error processing page {page_num}: {e}")

    return compressed_files


# Example usage
if __name__ == "__main__":
    times = time.time()
    input_pdf = "C:\\Users\\vignesh.vn\\Downloads\\S50MC_01.pdf"
    output_folder = "C:\\Users\\vignesh.vn\\Downloads\\pdf"
    files = split_and_compress_pdf_parallel(input_pdf, output_folder, max_workers=6)
    print(f"🎯 Done! {len(files)} compressed pages saved in {output_folder}")
    print(f"Time Taken: {time.time()-times:.2f}s")