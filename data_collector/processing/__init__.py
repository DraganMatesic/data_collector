"""PDF/OCR processing package for text extraction, document classification, and OCR training."""

from data_collector.processing.models import (
    OCRError,
    OCRResult,
    PageText,
    PDFClassification,
    PDFExtractionError,
    PDFTable,
    PDFTables,
    PDFText,
    PreprocessingError,
    ProcessingError,
    TrainingError,
)
from data_collector.processing.ocr import OCREngine, PaddleOCREngine, TesseractEngine, extract_text_ocr
from data_collector.processing.pdf import classify_pdf, extract_tables_pdfplumber, extract_text_pdfplumber
from data_collector.processing.preprocessing import ImagePreprocessor
from data_collector.processing.training import (
    AccuracyResult,
    TrainingDataset,
    TrainingDatasetSplit,
    TrainingSample,
    build_character_dictionary,
    evaluate_ocr_accuracy,
    generate_paddleocr_config,
)

__all__ = [
    "AccuracyResult",
    "ImagePreprocessor",
    "OCREngine",
    "OCRError",
    "OCRResult",
    "PaddleOCREngine",
    "PDFClassification",
    "PDFExtractionError",
    "PDFTable",
    "PDFTables",
    "PDFText",
    "PageText",
    "PreprocessingError",
    "ProcessingError",
    "TesseractEngine",
    "TrainingDataset",
    "TrainingDatasetSplit",
    "TrainingError",
    "TrainingSample",
    "build_character_dictionary",
    "classify_pdf",
    "evaluate_ocr_accuracy",
    "extract_tables_pdfplumber",
    "extract_text_ocr",
    "extract_text_pdfplumber",
    "generate_paddleocr_config",
]
