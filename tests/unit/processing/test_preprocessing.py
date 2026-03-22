"""Unit tests for ImagePreprocessor pipeline."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from PIL import Image

from data_collector.processing.models import PreprocessingError
from data_collector.processing.preprocessing import ImagePreprocessor

MODULE = "data_collector.processing.preprocessing"


def _color_image() -> Image.Image:
    """Create a small 100x100 RGB PIL Image for testing."""
    return Image.fromarray(np.zeros((100, 100, 3), dtype=np.uint8))


def _grayscale_image() -> Image.Image:
    """Create a small 100x100 grayscale PIL Image for testing."""
    return Image.fromarray(np.zeros((100, 100), dtype=np.uint8))


def _gray_array() -> np.ndarray:  # type: ignore[type-arg]
    """Return a 2D numpy array simulating a grayscale image."""
    return np.zeros((100, 100), dtype=np.uint8)


def _color_array() -> np.ndarray:  # type: ignore[type-arg]
    """Return a 3D numpy array simulating a BGR image."""
    return np.zeros((100, 100, 3), dtype=np.uint8)


class TestImagePreprocessor:
    """Tests for the ImagePreprocessor fixed-stage pipeline."""

    @patch(f"{MODULE}.cv2")
    def test_full_pipeline_runs_all_stages(self, mock_cv2: MagicMock) -> None:
        """All stages enabled by default. Verify every cv2 function is called."""
        gray = _gray_array()
        # _pil_to_cv2 calls cvtColor(RGB2BGR) for 3-channel input, then grayscale calls cvtColor(BGR2GRAY)
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.resize.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor()
        result = preprocessor.run(_color_image(), dpi=150)

        assert isinstance(result, Image.Image)
        mock_cv2.cvtColor.assert_called()
        mock_cv2.resize.assert_called()
        mock_cv2.fastNlMeansDenoising.assert_called()
        mock_cv2.createCLAHE.assert_called()
        mock_cv2.threshold.assert_called()
        mock_cv2.morphologyEx.assert_called()
        mock_cv2.copyMakeBorder.assert_called()

    @patch(f"{MODULE}.cv2")
    def test_selective_stage_disabling(self, mock_cv2: MagicMock) -> None:
        """Disable grayscale, denoise, and border. Those cv2 functions must not be called."""
        gray = _gray_array()
        # _pil_to_cv2 still calls cvtColor for RGB->BGR
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.resize.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray

        preprocessor = ImagePreprocessor(grayscale=False, denoise=False, border=False)
        preprocessor.run(_color_image(), dpi=150)

        mock_cv2.fastNlMeansDenoising.assert_not_called()
        mock_cv2.fastNlMeansDenoisingColored.assert_not_called()
        mock_cv2.copyMakeBorder.assert_not_called()

    @patch(f"{MODULE}.cv2")
    def test_dpi_none_skips_upscale(self, mock_cv2: MagicMock) -> None:
        """When dpi is None, cv2.resize must not be called."""
        gray = _gray_array()
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor()
        preprocessor.run(_color_image(), dpi=None)

        mock_cv2.resize.assert_not_called()

    @patch(f"{MODULE}.cv2")
    def test_upscale_skipped_when_dpi_sufficient(self, mock_cv2: MagicMock) -> None:
        """DPI at or above min_dpi means no resize needed."""
        gray = _gray_array()
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor(min_dpi=300)
        preprocessor.run(_color_image(), dpi=400)

        mock_cv2.resize.assert_not_called()

    @patch(f"{MODULE}.cv2")
    def test_upscale_applied_when_dpi_low(self, mock_cv2: MagicMock) -> None:
        """DPI below min_dpi triggers cv2.resize."""
        gray = _gray_array()
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.resize.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor(min_dpi=300)
        preprocessor.run(_color_image(), dpi=150)

        mock_cv2.resize.assert_called_once()

    @patch(f"{MODULE}.cv2")
    def test_clahe_params_forwarded(self, mock_cv2: MagicMock) -> None:
        """Custom CLAHE parameters are forwarded to cv2.createCLAHE."""
        gray = _gray_array()
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.resize.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor(clahe_clip_limit=3.0, clahe_tile_size=16)
        preprocessor.run(_color_image(), dpi=150)

        mock_cv2.createCLAHE.assert_called_once_with(clipLimit=3.0, tileGridSize=(16, 16))

    @patch(f"{MODULE}.cv2")
    def test_cv2_error_wrapped_in_preprocessing_error(self, mock_cv2: MagicMock) -> None:
        """A cv2.error raised during processing is wrapped in PreprocessingError."""
        mock_cv2.error = type("error", (Exception,), {})
        mock_cv2.cvtColor.side_effect = mock_cv2.error("synthetic cv2 failure")

        preprocessor = ImagePreprocessor()

        with pytest.raises(PreprocessingError, match="OpenCV operation failed"):
            preprocessor.run(_color_image(), dpi=150)

    @patch(f"{MODULE}.cv2")
    def test_grayscale_idempotent(self, mock_cv2: MagicMock) -> None:
        """A 2D (already grayscale) image should not trigger cvtColor for grayscale conversion."""
        gray = _gray_array()
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor()
        preprocessor.run(_grayscale_image(), dpi=None)

        # cvtColor should NOT be called because:
        # - _pil_to_cv2 skips conversion for 2D arrays (no RGB2BGR needed)
        # - _grayscale_image already has shape (100, 100) so _grayscale_image stage skips
        mock_cv2.cvtColor.assert_not_called()

    @patch(f"{MODULE}.cv2")
    def test_color_denoising_branch(self, mock_cv2: MagicMock) -> None:
        """7.1.1: With grayscale=False and denoise=True, fastNlMeansDenoisingColored is used."""
        color = _color_array()
        mock_cv2.cvtColor.return_value = color
        mock_cv2.resize.return_value = color
        mock_cv2.fastNlMeansDenoisingColored.return_value = color
        mock_cv2.createCLAHE.return_value.apply.return_value = _gray_array()
        mock_cv2.threshold.return_value = (None, _gray_array())
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = _gray_array()
        mock_cv2.copyMakeBorder.return_value = _gray_array()

        preprocessor = ImagePreprocessor(grayscale=False, denoise=True)
        preprocessor.run(_color_image(), dpi=150)

        mock_cv2.fastNlMeansDenoisingColored.assert_called_once()
        mock_cv2.fastNlMeansDenoising.assert_not_called()

    @patch(f"{MODULE}.cv2")
    def test_generic_exception_wrapped_in_preprocessing_error(self, mock_cv2: MagicMock) -> None:
        """7.1.4: A generic RuntimeError is wrapped in PreprocessingError with unexpected message."""
        mock_cv2.error = type("error", (Exception,), {})
        mock_cv2.cvtColor.side_effect = RuntimeError("something went wrong")

        preprocessor = ImagePreprocessor()

        with pytest.raises(PreprocessingError, match="Unexpected preprocessing failure"):
            preprocessor.run(_color_image(), dpi=150)

    @patch(f"{MODULE}.cv2")
    def test_deskew_with_significant_skew(self, mock_cv2: MagicMock) -> None:
        """7.1.5: When angle > 0.5 degrees, getRotationMatrix2D and warpAffine are called."""
        gray = _gray_array()
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.resize.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)

        # Create a mock contour with non-negligible area
        mock_contour = MagicMock()
        mock_cv2.findContours.return_value = ([mock_contour], None)
        mock_cv2.contourArea.return_value = 5000
        # minAreaRect returns ((cx, cy), (w, h), angle). Angle of 5.0 > 0.5 threshold
        mock_cv2.minAreaRect.return_value = ((50, 50), (80, 40), 5.0)
        mock_cv2.getRotationMatrix2D.return_value = np.eye(2, 3, dtype=np.float64)
        mock_cv2.warpAffine.return_value = gray
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor()
        preprocessor.run(_color_image(), dpi=150)

        mock_cv2.getRotationMatrix2D.assert_called_once()
        mock_cv2.warpAffine.assert_called_once()

    @patch(f"{MODULE}.cv2")
    def test_contrast_enhancement_multichannel(self, mock_cv2: MagicMock) -> None:
        """7.1.2: With grayscale=False and contrast=True, cvtColor is called inside _enhance_contrast."""
        color = _color_array()
        gray = _gray_array()
        # _pil_to_cv2 calls cvtColor for RGB2BGR, then _enhance_contrast calls cvtColor for BGR2GRAY
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.resize.return_value = color
        mock_cv2.fastNlMeansDenoisingColored.return_value = color
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        preprocessor = ImagePreprocessor(grayscale=False, contrast=True)
        preprocessor.run(_color_image(), dpi=150)

        # cvtColor should be called at least twice: once in _pil_to_cv2 (RGB2BGR) and
        # once in _enhance_contrast (BGR2GRAY) for multi-channel input
        assert mock_cv2.cvtColor.call_count >= 2
        mock_cv2.createCLAHE.assert_called_once()

    @patch(f"{MODULE}.cv2")
    def test_pil_to_cv2_rgba_image(self, mock_cv2: MagicMock) -> None:
        """7.1.6: _pil_to_cv2 handles a 4-channel RGBA image (no RGB2BGR conversion for channel!=3)."""
        gray = _gray_array()
        mock_cv2.cvtColor.return_value = gray
        mock_cv2.fastNlMeansDenoising.return_value = gray
        mock_cv2.createCLAHE.return_value.apply.return_value = gray
        mock_cv2.threshold.return_value = (None, gray)
        mock_cv2.findContours.return_value = ([], None)
        mock_cv2.morphologyEx.return_value = gray
        mock_cv2.copyMakeBorder.return_value = gray

        # Create a 4-channel RGBA image. _pil_to_cv2 should NOT call cvtColor for RGB2BGR
        # because the condition checks shape[2]==3, and RGBA has shape[2]==4.
        rgba_array = np.zeros((100, 100, 4), dtype=np.uint8)
        rgba_image = Image.fromarray(rgba_array, mode="RGBA")

        preprocessor = ImagePreprocessor()
        preprocessor.run(rgba_image, dpi=None)

        # cvtColor should be called for grayscale conversion (BGR2GRAY) from the grayscale stage,
        # but NOT for RGB2BGR in _pil_to_cv2 since shape[2]==4
        # First call to cvtColor would be from _grayscale_image (not _pil_to_cv2)
        for call_args in mock_cv2.cvtColor.call_args_list:
            args = call_args[0]
            # None of the cvtColor calls should use COLOR_RGB2BGR
            assert args[1] != mock_cv2.COLOR_RGB2BGR

    @patch(f"{MODULE}.cv2")
    def test_preprocessing_error_passthrough(self, mock_cv2: MagicMock) -> None:
        """7.2.1: PreprocessingError raised by a stage passes through without double-wrapping."""
        mock_cv2.error = type("error", (Exception,), {})
        # Make cvtColor raise PreprocessingError directly
        mock_cv2.cvtColor.side_effect = PreprocessingError("Stage failed deliberately")

        preprocessor = ImagePreprocessor()

        with pytest.raises(PreprocessingError, match="Stage failed deliberately"):
            preprocessor.run(_color_image(), dpi=150)
