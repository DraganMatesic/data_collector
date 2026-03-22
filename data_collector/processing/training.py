"""Training utilities for OCR model fine-tuning.

Provides dataset management (loading, splitting, exporting), character dictionary
building, PaddleOCR training config generation, and OCR accuracy evaluation with
CER, WER, and exact-match metrics.
"""

from __future__ import annotations

import logging
import random
import shutil
from dataclasses import dataclass
from pathlib import Path

from PIL import Image

from data_collector.processing.models import TrainingError
from data_collector.processing.ocr import OCREngine
from data_collector.processing.preprocessing import ImagePreprocessor

logger = logging.getLogger(__name__)

_IMAGE_EXTENSIONS: frozenset[str] = frozenset({".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"})


@dataclass(frozen=True)
class TrainingSample:
    """A single training sample pairing an image with its ground-truth text.

    Attributes:
        image_path: Absolute or relative path to the image file.
        ground_truth: Expected OCR output text for the image.
    """

    image_path: Path
    ground_truth: str


@dataclass(frozen=True)
class TrainingDatasetSplit:
    """Result of splitting a training dataset into training and evaluation subsets.

    Attributes:
        training_samples: Immutable tuple of samples assigned to the training set.
        evaluation_samples: Immutable tuple of samples assigned to the evaluation set.
        training_count: Number of training samples.
        evaluation_count: Number of evaluation samples.
    """

    training_samples: tuple[TrainingSample, ...]
    evaluation_samples: tuple[TrainingSample, ...]
    training_count: int
    evaluation_count: int


@dataclass(frozen=True)
class AccuracyResult:
    """Aggregated OCR accuracy metrics over a set of test samples.

    Attributes:
        character_error_rate: Character Error Rate (total edit distance / total reference length).
        word_error_rate: Word Error Rate (total word-level edit distance / total word count).
        total_samples: Number of samples evaluated.
        correct_samples: Number of samples with exact string match.
        exact_match_rate: Fraction of samples with exact match (0.0-1.0).
        skipped_samples: Number of samples skipped due to errors (corrupt images, I/O failures).
    """

    character_error_rate: float
    word_error_rate: float
    total_samples: int
    correct_samples: int
    exact_match_rate: float
    skipped_samples: int


class TrainingDataset:
    """Manages a collection of OCR training samples with load, split, and export capabilities.

    Supports loading from PaddleOCR annotation format (tab-delimited) and Tesseract
    paired-file format (image + .gt.txt). Provides deterministic splitting and export
    to both PaddleOCR and Tesseract training directory structures.

    Args:
        samples: Initial list of TrainingSample instances.
    """

    def __init__(self, samples: list[TrainingSample]) -> None:
        self._samples: list[TrainingSample] = list(samples)

    @property
    def samples(self) -> list[TrainingSample]:
        """Return a copy of the internal sample list.

        Returns:
            New list containing all TrainingSample instances.
        """
        return list(self._samples)

    @property
    def sample_count(self) -> int:
        """Return the number of samples in the dataset.

        Returns:
            Total number of training samples.
        """
        return len(self._samples)

    @classmethod
    def from_directory(cls, image_directory: Path, annotation_file: Path) -> TrainingDataset:
        """Load a dataset from a PaddleOCR-format annotation file.

        Each line in the annotation file has the format: ``image_path\\tground_truth``.
        Image paths are resolved relative to ``image_directory``. Empty lines are skipped.
        Samples whose resolved image path falls outside ``image_directory`` are skipped
        with a warning (path traversal protection).

        Args:
            image_directory: Base directory containing the training images.
            annotation_file: Path to the tab-delimited annotation file.

        Returns:
            TrainingDataset populated with samples from the annotation file.
        """
        loaded_samples: list[TrainingSample] = []
        annotation_text = annotation_file.read_text(encoding="utf-8")
        resolved_image_directory = image_directory.resolve()

        for line_number, line in enumerate(annotation_text.splitlines(), start=1):
            stripped_line = line.strip()
            if not stripped_line:
                continue

            parts = stripped_line.split("\t", maxsplit=1)
            if len(parts) != 2:
                logger.warning(f"Skipping malformed line {line_number} in {annotation_file}: missing tab separator")
                continue

            relative_path, ground_truth = parts
            image_path = image_directory / relative_path

            if not image_path.resolve().is_relative_to(resolved_image_directory):
                logger.warning(
                    f"Skipping line {line_number} in {annotation_file}: "
                    f"path traversal detected ({relative_path})"
                )
                continue

            loaded_samples.append(TrainingSample(image_path=image_path, ground_truth=ground_truth))

        logger.info(f"Loaded {len(loaded_samples)} samples from {annotation_file}")
        return cls(loaded_samples)

    @classmethod
    def from_paired_files(cls, image_directory: Path) -> TrainingDataset:
        """Load a dataset from Tesseract-style paired files.

        Scans ``image_directory`` for image files (.png, .jpg, .jpeg, .tif, .tiff, .bmp)
        and pairs each with a corresponding ``.gt.txt`` file containing the ground-truth text.
        Images without a matching ``.gt.txt`` file are skipped with a warning.

        Args:
            image_directory: Directory containing image files and their ``.gt.txt`` counterparts.

        Returns:
            TrainingDataset populated with paired samples.
        """
        loaded_samples: list[TrainingSample] = []

        image_files = sorted(
            file_path
            for file_path in image_directory.iterdir()
            if file_path.is_file() and file_path.suffix.lower() in _IMAGE_EXTENSIONS
        )

        for image_file in image_files:
            ground_truth_file = image_file.with_suffix(".gt.txt")
            if not ground_truth_file.exists():
                logger.warning(f"No .gt.txt file found for {image_file}, skipping")
                continue

            ground_truth = ground_truth_file.read_text(encoding="utf-8").strip()
            loaded_samples.append(TrainingSample(image_path=image_file, ground_truth=ground_truth))

        logger.info(f"Loaded {len(loaded_samples)} paired samples from {image_directory}")
        return cls(loaded_samples)

    def split(self, train_ratio: float = 0.9, seed: int = 42) -> TrainingDatasetSplit:
        """Split the dataset into training and evaluation subsets.

        Uses a seeded random shuffle for deterministic, reproducible splits.

        Args:
            train_ratio: Fraction of samples to assign to the training set (0.0-1.0).
            seed: Random seed for reproducibility.

        Returns:
            TrainingDatasetSplit with training and evaluation samples.

        Raises:
            ValueError: If train_ratio is not between 0.0 and 1.0 inclusive.
        """
        if not 0.0 <= train_ratio <= 1.0:
            raise ValueError(f"train_ratio must be between 0.0 and 1.0 inclusive, got {train_ratio}")

        shuffled_samples = list(self._samples)
        random.Random(seed).shuffle(shuffled_samples)

        split_index = int(len(shuffled_samples) * train_ratio)
        training_samples = tuple(shuffled_samples[:split_index])
        evaluation_samples = tuple(shuffled_samples[split_index:])

        result = TrainingDatasetSplit(
            training_samples=training_samples,
            evaluation_samples=evaluation_samples,
            training_count=len(training_samples),
            evaluation_count=len(evaluation_samples),
        )

        logger.info(f"Split dataset: {result.training_count} training, {result.evaluation_count} evaluation")
        return result

    def export_paddleocr(
        self,
        output_directory: Path,
        *,
        split: TrainingDatasetSplit | None = None,
    ) -> Path:
        """Export the dataset to PaddleOCR recognition training format.

        Creates the following directory structure::

            output_directory/
                train/
                    images/       -- training images
                    rec_gt_train.txt  -- annotation file (relative_path\\ttext)
                eval/
                    images/       -- evaluation images
                    rec_gt_eval.txt   -- annotation file (relative_path\\ttext)
                dict.txt          -- character dictionary

        Images are copied into the respective ``images/`` subdirectories. Annotation
        files use paths relative to the ``train/`` or ``eval/`` directory.

        Args:
            output_directory: Root directory for the exported dataset.
            split: Pre-computed dataset split. When None, calls ``self.split()`` with defaults.

        Returns:
            The output_directory path.

        Raises:
            TrainingError: If an image cannot be copied during export.
        """
        dataset_split = split if split is not None else self.split()

        training_images_directory = output_directory / "train" / "images"
        evaluation_images_directory = output_directory / "eval" / "images"
        training_images_directory.mkdir(parents=True, exist_ok=True)
        evaluation_images_directory.mkdir(parents=True, exist_ok=True)

        training_annotation_path = output_directory / "train" / "rec_gt_train.txt"
        evaluation_annotation_path = output_directory / "eval" / "rec_gt_eval.txt"

        try:
            _write_paddleocr_annotations(
                dataset_split.training_samples, training_images_directory, training_annotation_path
            )
            _write_paddleocr_annotations(
                dataset_split.evaluation_samples, evaluation_images_directory, evaluation_annotation_path
            )
        except TrainingError:
            raise
        except Exception as error:
            raise TrainingError(f"PaddleOCR export failed: {error}") from error

        character_dictionary = build_character_dictionary(self)
        dictionary_path = output_directory / "dict.txt"
        dictionary_path.write_text("\n".join(character_dictionary) + "\n", encoding="utf-8")

        logger.info(
            f"Exported PaddleOCR dataset to {output_directory}: "
            f"{dataset_split.training_count} training, {dataset_split.evaluation_count} evaluation, "
            f"{len(character_dictionary)} characters in dictionary"
        )
        return output_directory

    def export_tesseract(self, output_directory: Path, model_name: str) -> Path:
        """Export the dataset to Tesseract tesstrain format.

        Creates the following directory structure::

            output_directory/
                data/
                    {model_name}-ground-truth/
                        sample_000000.tif   -- image (converted to TIFF)
                        sample_000000.gt.txt -- ground-truth text

        Images are converted to TIFF format if they are not already. Each sample
        is named with a zero-padded index.

        Args:
            output_directory: Root directory for the exported dataset.
            model_name: Tesseract model name used as the ground-truth directory prefix.

        Returns:
            The output_directory path.

        Raises:
            TrainingError: If an image cannot be opened or converted during export.
        """
        ground_truth_directory = output_directory / "data" / f"{model_name}-ground-truth"
        ground_truth_directory.mkdir(parents=True, exist_ok=True)

        for sample_index, sample in enumerate(self._samples):
            base_name = f"sample_{sample_index:06d}"
            target_image_path = ground_truth_directory / f"{base_name}.tif"
            target_text_path = ground_truth_directory / f"{base_name}.gt.txt"

            try:
                if sample.image_path.suffix.lower() in {".tif", ".tiff"}:
                    shutil.copy2(sample.image_path, target_image_path)
                else:
                    with Image.open(sample.image_path) as source_image:
                        source_image.save(target_image_path, format="TIFF")

                target_text_path.write_text(sample.ground_truth, encoding="utf-8")
            except Exception as error:
                raise TrainingError(
                    f"Tesseract export failed at sample {sample_index} ({sample.image_path}): {error}",
                    source_path=str(sample.image_path),
                ) from error

        logger.info(f"Exported Tesseract dataset to {ground_truth_directory}: {self.sample_count} samples")
        return output_directory


def _write_paddleocr_annotations(
    samples: tuple[TrainingSample, ...],
    images_directory: Path,
    annotation_path: Path,
) -> None:
    """Copy images and write a PaddleOCR annotation file.

    Each image is copied to ``images_directory`` with a unique index-prefixed filename
    to prevent collisions from duplicate filenames. An annotation line is written
    with the relative path (from the annotation file's parent) and ground-truth text.

    Args:
        samples: Tuple of training samples to export.
        images_directory: Directory where images are copied.
        annotation_path: Path to the annotation file to write.

    Raises:
        TrainingError: If an image cannot be copied.
    """
    annotation_lines: list[str] = []

    for index, sample in enumerate(samples):
        unique_filename = f"{index:06d}_{sample.image_path.name}"
        destination = images_directory / unique_filename

        try:
            shutil.copy2(sample.image_path, destination)
        except Exception as error:
            raise TrainingError(
                f"PaddleOCR annotation export failed at sample {index} ({sample.image_path}): {error}",
                source_path=str(sample.image_path),
            ) from error

        relative_path = destination.relative_to(annotation_path.parent)
        annotation_lines.append(f"{relative_path}\t{sample.ground_truth}")

    annotation_path.write_text("\n".join(annotation_lines) + "\n", encoding="utf-8")


def build_character_dictionary(
    samples: list[TrainingSample] | TrainingDataset,
    *,
    include_special: bool = True,
) -> list[str]:
    """Build a sorted character dictionary from training samples.

    Extracts all unique characters from ground-truth strings and sorts them by
    Unicode codepoint. When ``include_special`` is True, ensures that space and
    common punctuation characters are present even if not found in the samples.

    Args:
        samples: List of TrainingSample instances or a TrainingDataset.
        include_special: Whether to include space and common punctuation characters.

    Returns:
        Sorted list of unique characters.
    """
    sample_list = samples.samples if isinstance(samples, TrainingDataset) else samples

    characters: set[str] = set()
    for sample in sample_list:
        characters.update(sample.ground_truth)

    if include_special:
        special_characters = {" ", ".", ",", "!", "?", ";", ":", "'", '"', "-", "(", ")", "/", "&", "@", "#"}
        characters.update(special_characters)

    sorted_characters = sorted(characters, key=ord)
    logger.info(f"Built character dictionary with {len(sorted_characters)} characters")
    return sorted_characters


def generate_paddleocr_config(
    model_name: str,
    dictionary_path: Path,
    training_data_path: Path,
    evaluation_data_path: Path,
    *,
    base_model: str = "PP-OCRv5",
    language: str = "en",
    epochs: int = 100,
    learning_rate: float = 0.0001,
    batch_size: int = 64,
    output_directory: Path | None = None,
) -> Path:
    """Generate a PaddleOCR YAML training configuration file.

    Writes a YAML config suitable for PaddleOCR's ``tools/train.py``. The config
    includes Global settings (model name, pretrained model, dictionary, epochs),
    Train/Eval data loader sections, and Optimizer settings.

    No PyYAML dependency is required; the config is written as a formatted string.

    Args:
        model_name: Name for the trained model (used in output paths and config filename).
        dictionary_path: Path to the character dictionary file (dict.txt).
        training_data_path: Path to the training annotation file (e.g., rec_gt_train.txt).
        evaluation_data_path: Path to the evaluation annotation file (e.g., rec_gt_eval.txt).
        base_model: PaddleOCR base model identifier for pretrained weights.
        language: Language code for the model.
        epochs: Number of training epochs (must be >= 1).
        learning_rate: Initial learning rate for the optimizer (must be > 0).
        batch_size: Training batch size (must be >= 1).
        output_directory: Directory to write the config file. Defaults to training_data_path's parent.

    Returns:
        Path to the generated YAML configuration file.

    Raises:
        TrainingError: If validation fails or config file cannot be written.
    """
    if epochs < 1:
        raise TrainingError(f"epochs must be >= 1, got {epochs}")
    if learning_rate <= 0:
        raise TrainingError(f"learning_rate must be > 0, got {learning_rate}")
    if batch_size < 1:
        raise TrainingError(f"batch_size must be >= 1, got {batch_size}")

    if output_directory is None:
        output_directory = training_data_path.parent

    output_directory.mkdir(parents=True, exist_ok=True)

    config_path = output_directory / f"{model_name}_config.yml"

    training_data_directory = training_data_path.parent
    evaluation_data_directory = evaluation_data_path.parent

    try:
        config_content = (
            f"Global:\n"
            f"  use_gpu: true\n"
            f"  epoch_num: {epochs}\n"
            f"  log_smooth_window: 20\n"
            f"  print_batch_step: 10\n"
            f"  save_model_dir: ./output/{model_name}\n"
            f"  save_epoch_step: 10\n"
            f"  eval_batch_step:\n"
            f"    - 0\n"
            f"    - 2000\n"
            f"  cal_metric_during_train: true\n"
            f"  pretrained_model: {base_model}\n"
            f"  character_dict_path: {dictionary_path}\n"
            f"  use_space_char: true\n"
            f"  max_text_length: 25\n"
            f"  infer_img: null\n"
            f"  character_type: {language}\n"
            f"  model_name: {model_name}\n"
            f"\n"
            f"Optimizer:\n"
            f"  name: Adam\n"
            f"  beta1: 0.9\n"
            f"  beta2: 0.999\n"
            f"  lr:\n"
            f"    name: Cosine\n"
            f"    learning_rate: {learning_rate}\n"
            f"    warmup_epoch: 5\n"
            f"  regularizer:\n"
            f"    name: L2\n"
            f"    factor: 0.00001\n"
            f"\n"
            f"Architecture:\n"
            f"  model_type: rec\n"
            f"  algorithm: SVTR_LCNet\n"
            f"  Transform: null\n"
            f"  Backbone:\n"
            f"    name: MobileNetV1Enhance\n"
            f"    scale: 0.5\n"
            f"    last_conv_stride:\n"
            f"      - 1\n"
            f"      - 2\n"
            f"    last_pool_type: avg\n"
            f"  Head:\n"
            f"    name: MultiHead\n"
            f"    head_list:\n"
            f"      - CTCHead:\n"
            f"          Neck:\n"
            f"            name: svtr\n"
            f"            dims: 64\n"
            f"            depth: 2\n"
            f"            hidden_dims: 120\n"
            f"            use_guide: true\n"
            f"          Head:\n"
            f"            fc_decay: 0.00001\n"
            f"      - SARHead:\n"
            f"          enc_dim: 512\n"
            f"          max_text_length: 25\n"
            f"\n"
            f"Loss:\n"
            f"  name: MultiLoss\n"
            f"  loss_config_list:\n"
            f"    - CTCLoss: null\n"
            f"    - SARLoss: null\n"
            f"\n"
            f"PostProcess:\n"
            f"  name: CTCLabelDecode\n"
            f"\n"
            f"Metric:\n"
            f"  name: RecMetric\n"
            f"  main_indicator: acc\n"
            f"\n"
            f"Train:\n"
            f"  dataset:\n"
            f"    name: SimpleDataSet\n"
            f"    data_dir: {training_data_directory}\n"
            f"    label_file_list:\n"
            f"      - {training_data_path}\n"
            f"    transforms:\n"
            f"      - DecodeImage:\n"
            f"          img_mode: BGR\n"
            f"          channel_first: false\n"
            f"      - RecAug: null\n"
            f"      - CTCLabelEncode: null\n"
            f"      - RecResizeImg:\n"
            f"          image_shape:\n"
            f"            - 3\n"
            f"            - 48\n"
            f"            - 320\n"
            f"      - KeepKeys:\n"
            f"          keep_keys:\n"
            f"            - image\n"
            f"            - label\n"
            f"            - length\n"
            f"  loader:\n"
            f"    shuffle: true\n"
            f"    batch_size_per_card: {batch_size}\n"
            f"    drop_last: true\n"
            f"    num_workers: 4\n"
            f"\n"
            f"Eval:\n"
            f"  dataset:\n"
            f"    name: SimpleDataSet\n"
            f"    data_dir: {evaluation_data_directory}\n"
            f"    label_file_list:\n"
            f"      - {evaluation_data_path}\n"
            f"    transforms:\n"
            f"      - DecodeImage:\n"
            f"          img_mode: BGR\n"
            f"          channel_first: false\n"
            f"      - CTCLabelEncode: null\n"
            f"      - RecResizeImg:\n"
            f"          image_shape:\n"
            f"            - 3\n"
            f"            - 48\n"
            f"            - 320\n"
            f"      - KeepKeys:\n"
            f"          keep_keys:\n"
            f"            - image\n"
            f"            - label\n"
            f"            - length\n"
            f"  loader:\n"
            f"    shuffle: false\n"
            f"    batch_size_per_card: {batch_size}\n"
            f"    drop_last: false\n"
            f"    num_workers: 4\n"
        )

        config_path.write_text(config_content, encoding="utf-8")
    except TrainingError:
        raise
    except Exception as error:
        raise TrainingError(f"Failed to generate PaddleOCR config: {error}") from error

    logger.info(f"Generated PaddleOCR config at {config_path}")
    return config_path


def evaluate_ocr_accuracy(
    engine: OCREngine,
    test_samples: list[TrainingSample],
    *,
    additional_preprocessor: ImagePreprocessor | None = None,
) -> AccuracyResult:
    """Evaluate OCR engine accuracy against ground-truth samples.

    For each test sample, opens the image, optionally applies additional preprocessing,
    runs OCR via the provided engine, and compares the predicted text against the ground
    truth. Corrupt or unreadable samples are skipped with a warning.

    When ``additional_preprocessor`` is provided, it runs on the image before the engine's
    own preprocessor (if any). This means both preprocessors will execute in sequence.

    Computes three metrics:
    - **CER** (Character Error Rate): total character-level edit distance divided by
      total ground-truth character count.
    - **WER** (Word Error Rate): total word-level edit distance divided by total
      ground-truth word count.
    - **Exact Match Rate**: fraction of samples where predicted text exactly matches
      ground truth (after stripping whitespace).

    Args:
        engine: OCR engine instance to evaluate.
        test_samples: List of TrainingSample instances with image paths and ground truth.
        additional_preprocessor: Optional additional preprocessing applied before the engine's
            own preprocessor. Both will run in sequence if the engine also has a preprocessor.

    Returns:
        AccuracyResult with CER, WER, total/correct sample counts, exact match rate,
        and number of skipped samples.

    Raises:
        TrainingError: If evaluation fails for a reason other than individual sample errors.
    """
    total_character_distance = 0
    total_character_count = 0
    total_word_distance = 0
    total_word_count = 0
    correct_samples = 0
    skipped_samples = 0

    for sample_index, sample in enumerate(test_samples):
        try:
            with Image.open(sample.image_path) as image:
                if additional_preprocessor is not None:
                    image = additional_preprocessor.run(image)

                ocr_result = engine.extract_text(image)

            predicted_text = ocr_result.text.strip()
            reference_text = sample.ground_truth.strip()

            character_distance = _edit_distance(predicted_text, reference_text)
            total_character_distance += character_distance
            total_character_count += len(reference_text) if reference_text else 1

            predicted_words = predicted_text.split()
            reference_words = reference_text.split()
            word_distance = _edit_distance_words(predicted_words, reference_words)
            total_word_distance += word_distance
            total_word_count += len(reference_words) if reference_words else 1

            if predicted_text == reference_text:
                correct_samples += 1
        except Exception as error:
            logger.warning(
                f"Skipping sample {sample_index} ({sample.image_path}): "
                f"{type(error).__name__}: {error}"
            )
            skipped_samples += 1
            continue

        if (sample_index + 1) % 100 == 0:
            logger.info(f"Evaluated {sample_index + 1}/{len(test_samples)} samples")

    evaluated_samples = len(test_samples) - skipped_samples
    character_error_rate = total_character_distance / total_character_count if total_character_count > 0 else 0.0
    word_error_rate = total_word_distance / total_word_count if total_word_count > 0 else 0.0
    exact_match_rate = correct_samples / evaluated_samples if evaluated_samples > 0 else 0.0

    result = AccuracyResult(
        character_error_rate=character_error_rate,
        word_error_rate=word_error_rate,
        total_samples=evaluated_samples,
        correct_samples=correct_samples,
        exact_match_rate=exact_match_rate,
        skipped_samples=skipped_samples,
    )

    logger.info(
        f"OCR evaluation complete: CER={result.character_error_rate:.4f}, "
        f"WER={result.word_error_rate:.4f}, exact_match={result.exact_match_rate:.4f} "
        f"({result.correct_samples}/{result.total_samples})"
        + (f", skipped={result.skipped_samples}" if result.skipped_samples > 0 else "")
    )
    return result


def _edit_distance(source: str, target: str) -> int:
    """Compute Levenshtein edit distance between two strings.

    Uses the standard dynamic programming algorithm with O(min(m, n)) space.

    Args:
        source: Source string.
        target: Target string.

    Returns:
        Minimum number of single-character insertions, deletions, and substitutions
        required to transform source into target.
    """
    source_length = len(source)
    target_length = len(target)

    if source_length == 0:
        return target_length
    if target_length == 0:
        return source_length

    if source_length > target_length:
        source, target = target, source
        source_length, target_length = target_length, source_length

    previous_row: list[int] = list(range(source_length + 1))
    current_row: list[int] = [0] * (source_length + 1)

    for target_index in range(1, target_length + 1):
        current_row[0] = target_index
        for source_index in range(1, source_length + 1):
            deletion_cost = previous_row[source_index] + 1
            insertion_cost = current_row[source_index - 1] + 1
            substitution_cost = previous_row[source_index - 1]
            if source[source_index - 1] != target[target_index - 1]:
                substitution_cost += 1
            current_row[source_index] = min(deletion_cost, insertion_cost, substitution_cost)
        previous_row, current_row = current_row, previous_row

    return previous_row[source_length]


def _edit_distance_words(source_words: list[str], target_words: list[str]) -> int:
    """Compute Levenshtein edit distance between two word sequences.

    Same algorithm as character-level edit distance but operates on lists of words
    instead of individual characters.

    Args:
        source_words: Source word sequence.
        target_words: Target word sequence.

    Returns:
        Minimum number of word-level insertions, deletions, and substitutions
        required to transform source_words into target_words.
    """
    source_length = len(source_words)
    target_length = len(target_words)

    if source_length == 0:
        return target_length
    if target_length == 0:
        return source_length

    if source_length > target_length:
        source_words, target_words = target_words, source_words
        source_length, target_length = target_length, source_length

    previous_row: list[int] = list(range(source_length + 1))
    current_row: list[int] = [0] * (source_length + 1)

    for target_index in range(1, target_length + 1):
        current_row[0] = target_index
        for source_index in range(1, source_length + 1):
            deletion_cost = previous_row[source_index] + 1
            insertion_cost = current_row[source_index - 1] + 1
            substitution_cost = previous_row[source_index - 1]
            if source_words[source_index - 1] != target_words[target_index - 1]:
                substitution_cost += 1
            current_row[source_index] = min(deletion_cost, insertion_cost, substitution_cost)
        previous_row, current_row = current_row, previous_row

    return previous_row[source_length]
