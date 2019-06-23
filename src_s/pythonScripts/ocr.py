import pytesseract
from wand.image import Image
from PIL import Image as PI
import io
import sys
#convert pdf to image
image_pdf = Image(filename='D:/development/workspace/test/src_s/file/invoice/input/2082881 - final (Citizens).pdf', resolution=400)
image_jpeg = image_pdf.convert('jpeg')
image_jpeg.save(filename='D:/development/workspace/test/src_s/file/2082881 - final (Citizens).jpeg')

# open image && OCR
# image = PI.open('D:/development/workspace/test/src_s/file/invoice/output/test.jpeg')
# code = pytesseract.image_to_string(image)
# print(code)
