"""Fixed-stage image preprocessing pipeline for OCR quality improvement.

Uses OpenCV for image transformations and Pillow for format conversion.
The pipeline executes eight stages in a fixed order, with individual stages
toggled via constructor flags.
"""

from __future__ import annotations

import logging

import cv2
import numpy as np
from numpy.typing import NDArray
from PIL import Image

from data_collector.processing.models import PreprocessingError

logger = logging.getLogger(__name__)


class ImagePreprocessor:
    """Fixed-stage image preprocessing pipeline for OCR.

    Pipeline stages execute in order: grayscale -> upscale -> denoise -> contrast ->
    deskew -> binarize -> morphology -> border. Individual stages can be skipped via
    constructor flags.

    Args:
        min_dpi: Minimum target DPI for upscaling. Images below this DPI are scaled up.
        grayscale: Whether to convert the image to grayscale.
        upscale: Whether to upscale low-DPI images.
        denoise: Whether to apply non-local means denoising.
        contrast: Whether to apply CLAHE contrast enhancement.
        deskew: Whether to correct image skew via contour analysis.
        binarize: Whether to apply Otsu binarization.
        morphology: Whether to apply morphological opening and closing.
        border: Whether to add a white border around the image.
        clahe_clip_limit: Contrast limiting threshold for CLAHE.
        clahe_tile_size: Size of the grid tiles for CLAHE (square grid).
    """

    def __init__(
        self,
        *,
        min_dpi: int = 300,
        grayscale: bool = True,
        upscale: bool = True,
        denoise: bool = True,
        contrast: bool = True,
        deskew: bool = True,
        binarize: bool = True,
        morphology: bool = True,
        border: bool = True,
        clahe_clip_limit: float = 2.0,
        clahe_tile_size: int = 8,
    ) -> None:
        self._min_dpi = min_dpi
        self._grayscale = grayscale
        self._upscale = upscale
        self._denoise = denoise
        self._contrast = contrast
        self._deskew = deskew
        self._binarize = binarize
        self._morphology = morphology
        self._border = border
        self._clahe_clip_limit = clahe_clip_limit
        self._clahe_tile_size = clahe_tile_size

    def run(self, image: Image.Image, dpi: int | None = None) -> Image.Image:
        """Execute the preprocessing pipeline on a PIL Image.

        Args:
            image: Input PIL Image to preprocess.
            dpi: Current DPI of the image. Required for upscaling; ignored if upscale is disabled.

        Returns:
            Preprocessed PIL Image ready for OCR.

        Raises:
            PreprocessingError: If any OpenCV operation fails during processing.
        """
        try:
            image_array = self._pil_to_cv2(image)

            if self._grayscale:
                logger.debug(f"Preprocessing stage: grayscale (shape={image_array.shape})")
                image_array = self._grayscale_image(image_array)

            if self._upscale:
                logger.debug(f"Preprocessing stage: upscale (dpi={dpi}, min_dpi={self._min_dpi})")
                image_array = self._upscale_image(image_array, dpi)

            if self._denoise:
                logger.debug(f"Preprocessing stage: denoise (shape={image_array.shape})")
                image_array = self._denoise_image(image_array)

            if self._contrast:
                logger.debug(f"Preprocessing stage: contrast (clip={self._clahe_clip_limit})")
                image_array = self._enhance_contrast(image_array)

            if self._deskew:
                logger.debug(f"Preprocessing stage: deskew (shape={image_array.shape})")
                image_array = self._deskew_image(image_array)

            if self._binarize:
                logger.debug(f"Preprocessing stage: binarize (shape={image_array.shape})")
                image_array = self._binarize_image(image_array)

            if self._morphology:
                logger.debug(f"Preprocessing stage: morphology (shape={image_array.shape})")
                image_array = self._apply_morphology(image_array)

            if self._border:
                logger.debug(f"Preprocessing stage: border (shape={image_array.shape})")
                image_array = self._add_border(image_array)

            return self._cv2_to_pil(image_array)

        except PreprocessingError:
            raise
        except cv2.error as opencv_error:
            raise PreprocessingError(f"OpenCV operation failed: {opencv_error}") from opencv_error
        except Exception as unexpected_error:
            raise PreprocessingError(f"Unexpected preprocessing failure: {unexpected_error}") from unexpected_error

    def _grayscale_image(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Convert image to grayscale if not already single-channel.

        Args:
            image_array: Input image as a numpy array.

        Returns:
            Single-channel grayscale image.
        """
        if len(image_array.shape) == 2:
            return image_array
        result: NDArray[np.uint8] = cv2.cvtColor(image_array, cv2.COLOR_BGR2GRAY)
        return result

    def _upscale_image(self, image_array: NDArray[np.uint8], current_dpi: int | None) -> NDArray[np.uint8]:
        """Upscale image if current DPI is below the minimum threshold.

        Args:
            image_array: Input image as a numpy array.
            current_dpi: Current DPI of the image. If None, upscaling is skipped.

        Returns:
            Upscaled image, or the original if no upscaling is needed.
        """
        if current_dpi is None or current_dpi >= self._min_dpi:
            return image_array

        scale_factor = self._min_dpi / current_dpi
        new_width = int(image_array.shape[1] * scale_factor)
        new_height = int(image_array.shape[0] * scale_factor)
        logger.debug(f"Upscaling from {current_dpi} DPI to {self._min_dpi} DPI (scale={scale_factor:.2f})")
        result: NDArray[np.uint8] = cv2.resize(image_array, (new_width, new_height), interpolation=cv2.INTER_CUBIC)
        return result

    def _denoise_image(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Apply non-local means denoising.

        Uses fastNlMeansDenoising for grayscale images and fastNlMeansDenoisingColored
        for color images.

        Args:
            image_array: Input image as a numpy array.

        Returns:
            Denoised image.
        """
        if len(image_array.shape) == 2:
            result: NDArray[np.uint8] = cv2.fastNlMeansDenoising(
                image_array, None, h=10, templateWindowSize=7, searchWindowSize=21
            )
        else:
            result = cv2.fastNlMeansDenoisingColored(image_array, None, h=10, hForColorComponents=10)
        return result

    def _enhance_contrast(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Apply CLAHE (Contrast Limited Adaptive Histogram Equalization).

        The image must be grayscale. If the image is multi-channel, it is converted
        to grayscale first.

        Args:
            image_array: Input image as a numpy array (should be grayscale).

        Returns:
            Contrast-enhanced grayscale image.
        """
        if len(image_array.shape) != 2:
            image_array = cv2.cvtColor(image_array, cv2.COLOR_BGR2GRAY)

        clahe = cv2.createCLAHE(
            clipLimit=self._clahe_clip_limit,
            tileGridSize=(self._clahe_tile_size, self._clahe_tile_size),
        )
        result: NDArray[np.uint8] = clahe.apply(image_array)
        return result

    def _deskew_image(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Correct image skew using contour-based angle detection.

        Converts to binary via thresholding, finds the largest contour, computes
        the minimum area rectangle angle, and rotates the image if the skew exceeds
        0.5 degrees. Uses white fill for border pixels created by rotation.

        Args:
            image_array: Input image as a numpy array.

        Returns:
            Deskewed image, or the original if skew is negligible.
        """
        working_image = image_array if len(image_array.shape) == 2 else cv2.cvtColor(image_array, cv2.COLOR_BGR2GRAY)

        _, binary_image = cv2.threshold(working_image, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        contours, _ = cv2.findContours(binary_image, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        if not contours:
            return image_array

        largest_contour = max(contours, key=cv2.contourArea)
        rotated_rectangle = cv2.minAreaRect(largest_contour)
        angle = rotated_rectangle[2]

        # minAreaRect returns angles in [-90, 0). Normalize to [-45, 45] range
        if angle < -45.0:
            angle = 90.0 + angle
        elif angle > 45.0:
            angle = angle - 90.0

        if abs(angle) <= 0.5:
            return image_array

        logger.debug(f"Deskew angle: {angle:.2f} degrees")
        height, width = image_array.shape[:2]
        center = (width / 2.0, height / 2.0)
        rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
        result: NDArray[np.uint8] = cv2.warpAffine(
            image_array, rotation_matrix, (width, height), borderMode=cv2.BORDER_CONSTANT, borderValue=255
        )
        return result

    def _binarize_image(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Apply Otsu binarization to produce a black-and-white image.

        Converts to grayscale first if the image is multi-channel.

        Args:
            image_array: Input image as a numpy array.

        Returns:
            Binary (black and white) image.
        """
        if len(image_array.shape) != 2:
            image_array = cv2.cvtColor(image_array, cv2.COLOR_BGR2GRAY)

        _, result = cv2.threshold(image_array, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        binary_image: NDArray[np.uint8] = result
        return binary_image

    def _apply_morphology(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Apply morphological opening followed by closing.

        Uses a 1x1 kernel to remove small noise artifacts while preserving
        character structure.

        Args:
            image_array: Input image as a numpy array.

        Returns:
            Morphologically cleaned image.
        """
        kernel: NDArray[np.uint8] = np.ones((1, 1), np.uint8)
        opened: NDArray[np.uint8] = cv2.morphologyEx(image_array, cv2.MORPH_OPEN, kernel)
        closed: NDArray[np.uint8] = cv2.morphologyEx(opened, cv2.MORPH_CLOSE, kernel)
        return closed

    def _add_border(self, image_array: NDArray[np.uint8]) -> NDArray[np.uint8]:
        """Add a 10-pixel white border around the image.

        Args:
            image_array: Input image as a numpy array.

        Returns:
            Image with white border added on all sides.
        """
        result: NDArray[np.uint8] = cv2.copyMakeBorder(
            image_array, 10, 10, 10, 10, cv2.BORDER_CONSTANT, value=255
        )
        return result

    @staticmethod
    def _pil_to_cv2(image: Image.Image) -> NDArray[np.uint8]:
        """Convert PIL Image to OpenCV BGR numpy array.

        Args:
            image: Input PIL Image.

        Returns:
            OpenCV-compatible numpy array (BGR for color, grayscale for single-channel).
        """
        image_array: NDArray[np.uint8] = np.array(image)
        if len(image_array.shape) == 3 and image_array.shape[2] == 3:
            image_array = cv2.cvtColor(image_array, cv2.COLOR_RGB2BGR)
        return image_array

    @staticmethod
    def _cv2_to_pil(image_array: NDArray[np.uint8]) -> Image.Image:
        """Convert OpenCV numpy array to PIL Image.

        Args:
            image_array: OpenCV image array (BGR for color, grayscale for single-channel).

        Returns:
            PIL Image in RGB or grayscale mode.
        """
        if len(image_array.shape) == 3 and image_array.shape[2] == 3:
            image_array = cv2.cvtColor(image_array, cv2.COLOR_BGR2RGB)
        return Image.fromarray(image_array)
