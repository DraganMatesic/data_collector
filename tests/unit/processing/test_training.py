"""Tests for OCR training dataset management, export, and evaluation utilities."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data_collector.processing.models import OCRResult, TrainingError
from data_collector.processing.training import (
    AccuracyResult,
    TrainingDataset,
    TrainingDatasetSplit,
    TrainingSample,
    build_character_dictionary,
    evaluate_ocr_accuracy,
    generate_paddleocr_config,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_dummy_image(path: Path) -> None:
    """Write a minimal valid PNG file (1x1 white pixel) to the given path."""
    # Minimal PNG: 8-byte signature + IHDR + IDAT + IEND
    # Using raw bytes avoids a PIL dependency in test helpers.
    png_bytes = (
        b"\x89PNG\r\n\x1a\n"  # PNG signature
        b"\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00\x90wS\xde"
        b"\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18\xd8N"
        b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    path.write_bytes(png_bytes)


def _make_samples(count: int) -> list[TrainingSample]:
    """Build a list of TrainingSample instances with sequential paths and labels."""
    return [
        TrainingSample(image_path=Path(f"/images/img_{index:04d}.png"), ground_truth=f"text_{index}")
        for index in range(count)
    ]


# ---------------------------------------------------------------------------
# TrainingSample
# ---------------------------------------------------------------------------


class TestTrainingSample:
    """Tests for the TrainingSample frozen dataclass."""

    def test_construction(self) -> None:
        sample = TrainingSample(image_path=Path("/data/image.png"), ground_truth="hello world")
        assert sample.image_path == Path("/data/image.png")
        assert sample.ground_truth == "hello world"

    def test_frozen(self) -> None:
        sample = TrainingSample(image_path=Path("/data/image.png"), ground_truth="hello")
        with pytest.raises(FrozenInstanceError):
            sample.image_path = Path("/other.png")  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            sample.ground_truth = "changed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# AccuracyResult
# ---------------------------------------------------------------------------


class TestAccuracyResult:
    """Tests for the AccuracyResult frozen dataclass."""

    def test_construction(self) -> None:
        result = AccuracyResult(
            character_error_rate=0.05,
            word_error_rate=0.10,
            total_samples=100,
            correct_samples=90,
            exact_match_rate=0.90,
            skipped_samples=3,
        )
        assert result.character_error_rate == pytest.approx(0.05)
        assert result.word_error_rate == pytest.approx(0.10)
        assert result.total_samples == 100
        assert result.correct_samples == 90
        assert result.exact_match_rate == pytest.approx(0.90)
        assert result.skipped_samples == 3

    def test_frozen(self) -> None:
        result = AccuracyResult(
            character_error_rate=0.0,
            word_error_rate=0.0,
            total_samples=1,
            correct_samples=1,
            exact_match_rate=1.0,
            skipped_samples=0,
        )
        with pytest.raises(FrozenInstanceError):
            result.character_error_rate = 0.5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TrainingDataset core
# ---------------------------------------------------------------------------


class TestTrainingDataset:
    """Tests for TrainingDataset construction and property access."""

    def test_construction(self) -> None:
        samples = _make_samples(5)
        dataset = TrainingDataset(samples)
        assert dataset.sample_count == 5

    def test_samples_returns_copy(self) -> None:
        samples = _make_samples(3)
        dataset = TrainingDataset(samples)

        returned = dataset.samples
        returned.pop()

        assert dataset.sample_count == 3, "Modifying the returned list must not affect the dataset"


# ---------------------------------------------------------------------------
# TrainingDataset.from_directory
# ---------------------------------------------------------------------------


class TestFromDirectory:
    """Tests for loading PaddleOCR-format annotation files."""

    def test_load_paddleocr_format(self, tmp_path: Path) -> None:
        image_directory = tmp_path / "images"
        image_directory.mkdir()

        _create_dummy_image(image_directory / "img_001.png")
        _create_dummy_image(image_directory / "img_002.png")

        annotation_file = tmp_path / "labels.txt"
        annotation_file.write_text("img_001.png\thello world\nimg_002.png\tfoo bar\n", encoding="utf-8")

        dataset = TrainingDataset.from_directory(image_directory, annotation_file)

        assert dataset.sample_count == 2
        assert dataset.samples[0].image_path == image_directory / "img_001.png"
        assert dataset.samples[0].ground_truth == "hello world"
        assert dataset.samples[1].image_path == image_directory / "img_002.png"
        assert dataset.samples[1].ground_truth == "foo bar"

    def test_skip_empty_lines(self, tmp_path: Path) -> None:
        image_directory = tmp_path / "images"
        image_directory.mkdir()

        _create_dummy_image(image_directory / "img_001.png")

        annotation_file = tmp_path / "labels.txt"
        annotation_file.write_text("\nimg_001.png\thello\n\n\n", encoding="utf-8")

        dataset = TrainingDataset.from_directory(image_directory, annotation_file)
        assert dataset.sample_count == 1

    def test_from_directory_malformed_line_skipped(self, tmp_path: Path) -> None:
        """7.1.13: A line missing the tab separator is skipped; valid lines are still loaded."""
        image_directory = tmp_path / "images"
        image_directory.mkdir()

        _create_dummy_image(image_directory / "img_001.png")
        _create_dummy_image(image_directory / "img_002.png")

        annotation_file = tmp_path / "labels.txt"
        # Line 1 has no tab, line 2 and 3 are valid
        annotation_file.write_text(
            "malformed_line_no_tab\nimg_001.png\thello world\nimg_002.png\tfoo bar\n",
            encoding="utf-8",
        )

        dataset = TrainingDataset.from_directory(image_directory, annotation_file)

        assert dataset.sample_count == 2
        assert dataset.samples[0].ground_truth == "hello world"
        assert dataset.samples[1].ground_truth == "foo bar"

    def test_from_directory_path_traversal_skipped(self, tmp_path: Path) -> None:
        """7.3.1: Annotation with path traversal (../../) is skipped with a warning."""
        image_directory = tmp_path / "images"
        image_directory.mkdir()

        _create_dummy_image(image_directory / "valid.png")

        annotation_file = tmp_path / "labels.txt"
        annotation_file.write_text(
            "../../etc/passwd\tmalicious text\nvalid.png\tgood text\n",
            encoding="utf-8",
        )

        dataset = TrainingDataset.from_directory(image_directory, annotation_file)

        # Path traversal line is skipped; only the valid line is loaded
        assert dataset.sample_count == 1
        assert dataset.samples[0].ground_truth == "good text"


# ---------------------------------------------------------------------------
# TrainingDataset.from_paired_files
# ---------------------------------------------------------------------------


class TestFromPairedFiles:
    """Tests for loading Tesseract-style paired image + .gt.txt files."""

    def test_load_tesseract_format(self, tmp_path: Path) -> None:
        _create_dummy_image(tmp_path / "receipt_001.png")
        (tmp_path / "receipt_001.gt.txt").write_text("Total: $42.00", encoding="utf-8")

        _create_dummy_image(tmp_path / "receipt_002.png")
        (tmp_path / "receipt_002.gt.txt").write_text("Tax: $3.50", encoding="utf-8")

        dataset = TrainingDataset.from_paired_files(tmp_path)

        assert dataset.sample_count == 2
        ground_truths = {sample.ground_truth for sample in dataset.samples}
        assert ground_truths == {"Total: $42.00", "Tax: $3.50"}

    def test_missing_gt_file_skipped(self, tmp_path: Path) -> None:
        _create_dummy_image(tmp_path / "image_with_gt.png")
        (tmp_path / "image_with_gt.gt.txt").write_text("has ground truth", encoding="utf-8")

        _create_dummy_image(tmp_path / "image_without_gt.png")
        # Intentionally no .gt.txt for this image

        dataset = TrainingDataset.from_paired_files(tmp_path)
        assert dataset.sample_count == 1
        assert dataset.samples[0].ground_truth == "has ground truth"


# ---------------------------------------------------------------------------
# TrainingDataset.split
# ---------------------------------------------------------------------------


class TestSplit:
    """Tests for deterministic dataset splitting."""

    def test_split_ratio(self) -> None:
        dataset = TrainingDataset(_make_samples(10))
        split_result = dataset.split(train_ratio=0.9, seed=42)

        assert isinstance(split_result, TrainingDatasetSplit)
        assert split_result.training_count == 9
        assert split_result.evaluation_count == 1
        assert split_result.training_count + split_result.evaluation_count == 10

    def test_deterministic_with_seed(self) -> None:
        dataset = TrainingDataset(_make_samples(20))
        first_split = dataset.split(train_ratio=0.8, seed=99)
        second_split = dataset.split(train_ratio=0.8, seed=99)

        assert first_split.training_samples == second_split.training_samples
        assert first_split.evaluation_samples == second_split.evaluation_samples

    def test_different_seed_different_split(self) -> None:
        dataset = TrainingDataset(_make_samples(20))
        split_seed_one = dataset.split(train_ratio=0.8, seed=1)
        split_seed_two = dataset.split(train_ratio=0.8, seed=2)

        assert split_seed_one.training_samples != split_seed_two.training_samples

    def test_split_empty_dataset(self) -> None:
        """7.1.14: Splitting an empty dataset returns empty tuples and zero counts."""
        dataset = TrainingDataset([])
        split_result = dataset.split(train_ratio=0.9, seed=42)

        assert split_result.training_samples == ()
        assert split_result.evaluation_samples == ()
        assert split_result.training_count == 0
        assert split_result.evaluation_count == 0

    def test_split_single_sample(self) -> None:
        """7.1.15: Splitting 1 sample with ratio=0.9 puts nothing in training, 1 in evaluation."""
        dataset = TrainingDataset(_make_samples(1))
        split_result = dataset.split(train_ratio=0.9, seed=42)

        # int(1 * 0.9) = 0, so split_index=0: training is empty, evaluation has 1 sample
        assert split_result.training_count == 0
        assert split_result.evaluation_count == 1
        assert len(split_result.evaluation_samples) == 1

    def test_split_invalid_ratio_negative(self) -> None:
        """7.3.1: Negative train_ratio raises ValueError."""
        dataset = TrainingDataset(_make_samples(10))

        with pytest.raises(ValueError, match="train_ratio must be between 0.0 and 1.0"):
            dataset.split(train_ratio=-0.5, seed=42)

    def test_split_invalid_ratio_above_one(self) -> None:
        """7.3.1: train_ratio > 1.0 raises ValueError."""
        dataset = TrainingDataset(_make_samples(10))

        with pytest.raises(ValueError, match="train_ratio must be between 0.0 and 1.0"):
            dataset.split(train_ratio=1.5, seed=42)


# ---------------------------------------------------------------------------
# TrainingDataset.export_paddleocr
# ---------------------------------------------------------------------------


class TestExportPaddleocr:
    """Tests for PaddleOCR training directory export."""

    def test_creates_directory_structure(self, tmp_path: Path) -> None:
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()
        samples: list[TrainingSample] = []
        for index in range(4):
            image_path = image_directory / f"img_{index:04d}.png"
            _create_dummy_image(image_path)
            samples.append(TrainingSample(image_path=image_path, ground_truth=f"text {index}"))

        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"
        dataset.export_paddleocr(output_directory)

        assert (output_directory / "train").is_dir()
        assert (output_directory / "eval").is_dir()
        assert (output_directory / "dict.txt").is_file()

    def test_annotation_files_written(self, tmp_path: Path) -> None:
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()
        samples: list[TrainingSample] = []
        for index in range(4):
            image_path = image_directory / f"img_{index:04d}.png"
            _create_dummy_image(image_path)
            samples.append(TrainingSample(image_path=image_path, ground_truth=f"label_{index}"))

        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"
        dataset.export_paddleocr(output_directory)

        training_annotation = output_directory / "train" / "rec_gt_train.txt"
        evaluation_annotation = output_directory / "eval" / "rec_gt_eval.txt"

        assert training_annotation.is_file()
        assert evaluation_annotation.is_file()

        training_lines = [
            line for line in training_annotation.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        evaluation_lines = [
            line for line in evaluation_annotation.read_text(encoding="utf-8").splitlines() if line.strip()
        ]

        assert len(training_lines) + len(evaluation_lines) == 4

        # Each annotation line must be tab-separated: relative_path<TAB>ground_truth
        for line in training_lines + evaluation_lines:
            parts = line.split("\t")
            assert len(parts) == 2, f"Expected tab-separated annotation, got: {line}"

    def test_export_paddleocr_empty_dataset(self, tmp_path: Path) -> None:
        """7.1.16: Exporting an empty dataset creates directories and empty annotation files."""
        dataset = TrainingDataset([])
        output_directory = tmp_path / "output"
        dataset.export_paddleocr(output_directory)

        assert (output_directory / "train").is_dir()
        assert (output_directory / "eval").is_dir()
        assert (output_directory / "dict.txt").is_file()
        assert (output_directory / "train" / "rec_gt_train.txt").is_file()
        assert (output_directory / "eval" / "rec_gt_eval.txt").is_file()

    def test_export_paddleocr_idempotent(self, tmp_path: Path) -> None:
        """7.3.1: Running export_paddleocr twice produces no errors on the second run."""
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()
        samples: list[TrainingSample] = []
        for index in range(3):
            image_path = image_directory / f"img_{index:04d}.png"
            _create_dummy_image(image_path)
            samples.append(TrainingSample(image_path=image_path, ground_truth=f"text {index}"))

        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"

        # First export
        dataset.export_paddleocr(output_directory)
        # Second export -- should not raise
        dataset.export_paddleocr(output_directory)

        assert (output_directory / "train").is_dir()
        assert (output_directory / "dict.txt").is_file()


# ---------------------------------------------------------------------------
# TrainingDataset.export_tesseract
# ---------------------------------------------------------------------------


class TestExportTesseract:
    """Tests for Tesseract tesstrain directory export."""

    def test_creates_ground_truth_directory(self, tmp_path: Path) -> None:
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()
        samples: list[TrainingSample] = []
        for index in range(3):
            image_path = image_directory / f"img_{index:04d}.png"
            _create_dummy_image(image_path)
            samples.append(TrainingSample(image_path=image_path, ground_truth=f"word_{index}"))

        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_image = MagicMock()
            mock_open.return_value = mock_image
            dataset.export_tesseract(output_directory, model_name="test_model")

        ground_truth_directory = output_directory / "data" / "test_model-ground-truth"
        assert ground_truth_directory.is_dir()

    def test_gt_txt_files_written(self, tmp_path: Path) -> None:
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()
        samples: list[TrainingSample] = []
        for index in range(3):
            image_path = image_directory / f"img_{index:04d}.png"
            _create_dummy_image(image_path)
            samples.append(TrainingSample(image_path=image_path, ground_truth=f"word_{index}"))

        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_image = MagicMock()
            mock_open.return_value = mock_image
            dataset.export_tesseract(output_directory, model_name="my_model")

        ground_truth_directory = output_directory / "data" / "my_model-ground-truth"

        for index in range(3):
            ground_truth_file = ground_truth_directory / f"sample_{index:06d}.gt.txt"
            assert ground_truth_file.is_file(), f"Expected {ground_truth_file} to exist"
            content = ground_truth_file.read_text(encoding="utf-8")
            assert content == f"word_{index}"

    def test_export_tesseract_tiff_copy(self, tmp_path: Path) -> None:
        """7.1.17: When source is .tif, shutil.copy2 is used instead of Image.open+save."""
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()

        tif_path = image_directory / "receipt.tif"
        _create_dummy_image(tif_path)  # Content doesn't matter; copy2 just copies bytes

        samples = [TrainingSample(image_path=tif_path, ground_truth="Total: $42.00")]
        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"

        with (
            patch("data_collector.processing.training.shutil.copy2") as mock_copy2,
            patch("data_collector.processing.training.Image.open") as mock_image_open,
        ):
            dataset.export_tesseract(output_directory, model_name="tif_model")

            mock_copy2.assert_called_once()
            mock_image_open.assert_not_called()

    def test_export_tesseract_saves_as_tiff(self, tmp_path: Path) -> None:
        """7.4.3: For non-TIFF source images, Image.save is called with format='TIFF'."""
        image_directory = tmp_path / "source_images"
        image_directory.mkdir()

        png_path = image_directory / "receipt.png"
        _create_dummy_image(png_path)

        samples = [TrainingSample(image_path=png_path, ground_truth="Total: $42.00")]
        dataset = TrainingDataset(samples)
        output_directory = tmp_path / "output"

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_image = MagicMock()
            # Source uses 'with Image.open(...) as source_image:', so mock context manager
            mock_open.return_value.__enter__ = MagicMock(return_value=mock_image)
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            dataset.export_tesseract(output_directory, model_name="png_model")

            mock_image.save.assert_called_once()
            # Source code calls source_image.save(target_image_path, format="TIFF")
            _save_args, save_kwargs = mock_image.save.call_args
            assert save_kwargs.get("format") == "TIFF"


# ---------------------------------------------------------------------------
# build_character_dictionary
# ---------------------------------------------------------------------------


class TestBuildCharacterDictionary:
    """Tests for building sorted character dictionaries from training samples."""

    def test_extracts_unique_characters(self) -> None:
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="abc"),
            TrainingSample(image_path=Path("/img/b.png"), ground_truth="bcd"),
        ]
        dictionary = build_character_dictionary(samples, include_special=False)
        assert dictionary == ["a", "b", "c", "d"]

    def test_includes_special_characters(self) -> None:
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="hello world"),
        ]
        dictionary = build_character_dictionary(samples, include_special=True)
        # "hello world" contains a space; include_special=True guarantees space is present
        assert " " in dictionary

    def test_accepts_training_dataset(self) -> None:
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="xyz"),
        ]
        dataset = TrainingDataset(samples)
        dictionary = build_character_dictionary(dataset, include_special=False)
        assert dictionary == ["x", "y", "z"]

    def test_build_dictionary_empty_samples(self) -> None:
        """7.1.18: Empty sample list with include_special=False returns empty list."""
        dictionary = build_character_dictionary([], include_special=False)
        assert dictionary == []

    def test_build_dictionary_empty_samples_with_special(self) -> None:
        """7.1.18: Empty sample list with include_special=True returns only special characters."""
        dictionary = build_character_dictionary([], include_special=True)
        assert len(dictionary) > 0
        assert " " in dictionary


# ---------------------------------------------------------------------------
# generate_paddleocr_config
# ---------------------------------------------------------------------------


class TestGeneratePaddleOcrConfig:
    """Tests for PaddleOCR YAML config file generation."""

    def test_generates_yaml_file(self, tmp_path: Path) -> None:
        dictionary_path = tmp_path / "dict.txt"
        dictionary_path.write_text("a\nb\nc\n", encoding="utf-8")

        training_data_path = tmp_path / "train"
        training_data_path.mkdir()
        evaluation_data_path = tmp_path / "eval"
        evaluation_data_path.mkdir()

        config_path = generate_paddleocr_config(
            model_name="my_ocr_model",
            dictionary_path=dictionary_path,
            training_data_path=training_data_path,
            evaluation_data_path=evaluation_data_path,
            learning_rate=0.0005,
            epochs=50,
            output_directory=tmp_path / "config_output",
        )

        assert config_path.is_file()
        config_content = config_path.read_text(encoding="utf-8")
        assert "my_ocr_model" in config_content
        assert "0.0005" in config_content
        assert "epoch_num: 50" in config_content

    def test_config_contains_paths(self, tmp_path: Path) -> None:
        dictionary_path = tmp_path / "dict.txt"
        dictionary_path.write_text("a\n", encoding="utf-8")

        training_data_path = tmp_path / "train"
        training_data_path.mkdir()
        evaluation_data_path = tmp_path / "eval"
        evaluation_data_path.mkdir()

        config_path = generate_paddleocr_config(
            model_name="path_test",
            dictionary_path=dictionary_path,
            training_data_path=training_data_path,
            evaluation_data_path=evaluation_data_path,
            output_directory=tmp_path / "config_output",
        )

        config_content = config_path.read_text(encoding="utf-8")
        assert str(dictionary_path) in config_content
        assert str(training_data_path) in config_content
        assert str(evaluation_data_path) in config_content

    def test_generate_config_default_output_directory(self, tmp_path: Path) -> None:
        """7.1.21: When output_directory=None, config is created in parent of training_data_path."""
        dictionary_path = tmp_path / "dict.txt"
        dictionary_path.write_text("a\nb\n", encoding="utf-8")

        training_data_path = tmp_path / "data" / "train"
        training_data_path.mkdir(parents=True)
        evaluation_data_path = tmp_path / "data" / "eval"
        evaluation_data_path.mkdir(parents=True)

        config_path = generate_paddleocr_config(
            model_name="default_dir_test",
            dictionary_path=dictionary_path,
            training_data_path=training_data_path,
            evaluation_data_path=evaluation_data_path,
            output_directory=None,
        )

        # Default output_directory is training_data_path.parent (tmp_path / "data")
        assert config_path.parent == training_data_path.parent
        assert config_path.is_file()
        assert config_path.name == "default_dir_test_config.yml"

    def test_generate_config_invalid_epochs(self, tmp_path: Path) -> None:
        """7.3.1: Negative epochs raises TrainingError."""
        dictionary_path = tmp_path / "dict.txt"
        dictionary_path.write_text("a\n", encoding="utf-8")

        training_data_path = tmp_path / "train"
        training_data_path.mkdir()
        evaluation_data_path = tmp_path / "eval"
        evaluation_data_path.mkdir()

        with pytest.raises(TrainingError, match="epochs must be >= 1"):
            generate_paddleocr_config(
                model_name="neg_epochs",
                dictionary_path=dictionary_path,
                training_data_path=training_data_path,
                evaluation_data_path=evaluation_data_path,
                epochs=-10,
                output_directory=tmp_path / "config_output",
            )

    def test_generate_config_invalid_batch_size(self, tmp_path: Path) -> None:
        """7.3.1: Zero batch_size raises TrainingError."""
        dictionary_path = tmp_path / "dict.txt"
        dictionary_path.write_text("a\n", encoding="utf-8")

        training_data_path = tmp_path / "train"
        training_data_path.mkdir()
        evaluation_data_path = tmp_path / "eval"
        evaluation_data_path.mkdir()

        with pytest.raises(TrainingError, match="batch_size must be >= 1"):
            generate_paddleocr_config(
                model_name="zero_batch",
                dictionary_path=dictionary_path,
                training_data_path=training_data_path,
                evaluation_data_path=evaluation_data_path,
                batch_size=0,
                output_directory=tmp_path / "config_output",
            )


# ---------------------------------------------------------------------------
# evaluate_ocr_accuracy
# ---------------------------------------------------------------------------


class TestEvaluateOcrAccuracy:
    """Tests for OCR accuracy evaluation with mocked engine and image loading."""

    def test_perfect_accuracy(self) -> None:
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="hello"),
            TrainingSample(image_path=Path("/img/b.png"), ground_truth="world"),
        ]

        mock_engine = MagicMock()
        mock_engine.extract_text.side_effect = [
            OCRResult(text="hello", engine_name="mock", preprocessed=False, confidence=1.0),
            OCRResult(text="world", engine_name="mock", preprocessed=False, confidence=1.0),
        ]

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_open.return_value = MagicMock()
            result = evaluate_ocr_accuracy(mock_engine, samples)

        assert result.character_error_rate == pytest.approx(0.0)
        assert result.word_error_rate == pytest.approx(0.0)
        assert result.exact_match_rate == pytest.approx(1.0)
        assert result.total_samples == 2
        assert result.correct_samples == 2

    def test_partial_accuracy(self) -> None:
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="hello"),
        ]

        # Return "hallo" instead of "hello" -- one character difference
        mock_engine = MagicMock()
        mock_engine.extract_text.return_value = OCRResult(
            text="hallo", engine_name="mock", preprocessed=False, confidence=0.8,
        )

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_open.return_value = MagicMock()
            result = evaluate_ocr_accuracy(mock_engine, samples)

        assert result.character_error_rate > 0.0
        assert result.exact_match_rate == pytest.approx(0.0)
        assert result.total_samples == 1
        assert result.correct_samples == 0

    def test_empty_result(self) -> None:
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="expected text"),
        ]

        mock_engine = MagicMock()
        mock_engine.extract_text.return_value = OCRResult(
            text="", engine_name="mock", preprocessed=False, confidence=0.0,
        )

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_open.return_value = MagicMock()
            result = evaluate_ocr_accuracy(mock_engine, samples)

        # Empty prediction against non-empty ground truth => high CER
        assert result.character_error_rate > 0.5
        assert result.exact_match_rate == pytest.approx(0.0)
        assert result.correct_samples == 0

    def test_evaluate_empty_samples(self) -> None:
        """7.1.19: Evaluate with empty sample list returns CER=0, WER=0, total=0."""
        mock_engine = MagicMock()

        result = evaluate_ocr_accuracy(mock_engine, [])

        assert result.character_error_rate == pytest.approx(0.0)
        assert result.word_error_rate == pytest.approx(0.0)
        assert result.total_samples == 0
        assert result.correct_samples == 0
        assert result.exact_match_rate == pytest.approx(0.0)
        mock_engine.extract_text.assert_not_called()

    def test_evaluate_with_preprocessor(self) -> None:
        """7.1.20: When additional_preprocessor is passed, it is called on each image before OCR."""
        samples = [
            TrainingSample(image_path=Path("/img/a.png"), ground_truth="hello"),
            TrainingSample(image_path=Path("/img/b.png"), ground_truth="world"),
        ]

        mock_engine = MagicMock()
        mock_engine.extract_text.side_effect = [
            OCRResult(text="hello", engine_name="mock", preprocessed=True, confidence=1.0),
            OCRResult(text="world", engine_name="mock", preprocessed=True, confidence=1.0),
        ]

        mock_preprocessor = MagicMock()
        mock_preprocessed_image = MagicMock()
        mock_preprocessor.run.return_value = mock_preprocessed_image

        with patch("data_collector.processing.training.Image.open") as mock_open:
            mock_image = MagicMock()
            mock_open.return_value.__enter__ = MagicMock(return_value=mock_image)
            mock_open.return_value.__exit__ = MagicMock(return_value=False)
            result = evaluate_ocr_accuracy(
                mock_engine, samples, additional_preprocessor=mock_preprocessor
            )

        assert mock_preprocessor.run.call_count == 2
        # The preprocessed image should be passed to the engine
        for engine_call in mock_engine.extract_text.call_args_list:
            assert engine_call[0][0] is mock_preprocessed_image
        assert result.total_samples == 2
        assert result.correct_samples == 2
