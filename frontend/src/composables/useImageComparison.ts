import { ref, computed } from "vue";
import type { ImageMetadata, ChangeMaskMetadata } from "@/types/api";
import { apiService } from "@/services/api";

interface ComparisonData {
  previousImage: ImageMetadata;
  currentImage: ImageMetadata;
  changeMask: ChangeMaskMetadata | null;
  previousImageUrl: string;
  currentImageUrl: string;
  changeMaskUrl: string | null;
}

export function useImageComparison() {
  const isLoading = ref(false);
  const hasError = ref(false);
  const errorMessage = ref("");
  const comparisonData = ref<ComparisonData | null>(null);

  /**
   * Find the closest images for two different years from the available images
   */
  function findComparisonImages(
    clickedImage: ImageMetadata,
    availableImages: ImageMetadata[],
    visibleYears: Set<number>
  ): { previousImage: ImageMetadata | null; currentImage: ImageMetadata } {
    const clickedYear = new Date(clickedImage.time).getFullYear();
    const clickedTime = new Date(clickedImage.time).getTime();
    
    // Sort visible years
    const sortedYears = Array.from(visibleYears).sort((a, b) => a - b);

    // If less than 2 years visible, can't do comparison
    if (sortedYears.length < 2) {
      console.warn('Less than 2 visible years, cannot do comparison');
      return { previousImage: null, currentImage: clickedImage };
    }
    
    // Find current and previous year
    let currentYear = clickedYear;
    let previousYear: number | null = null;
    
    // If the clicked year is in visible years, use it as current
    if (visibleYears.has(clickedYear)) {
      const yearIndex = sortedYears.indexOf(clickedYear);
      if (yearIndex > 0) {
        previousYear = sortedYears[yearIndex - 1];
      } else {
        // Clicked year is the earliest visible year, try to use next year as current
        if (sortedYears.length > 1) {
          currentYear = sortedYears[1];
          previousYear = sortedYears[0]; // Use clicked year as previous
        }
      }
    } else {
      // If clicked year is not visible, find the closest visible year as current
      const closestYear = sortedYears.reduce((closest, year) => {
        return Math.abs(year - clickedYear) < Math.abs(closest - clickedYear) ? year : closest;
      });
      currentYear = closestYear;
      
      const yearIndex = sortedYears.indexOf(currentYear);
      if (yearIndex > 0) {
        previousYear = sortedYears[yearIndex - 1];
      }
    }

    // Find the closest image for the current year (preferring the clicked image if it's in the current year)
    let currentImage = clickedImage;
    if (new Date(clickedImage.time).getFullYear() !== currentYear) {
      const currentYearImages = availableImages.filter(
        img => new Date(img.time).getFullYear() === currentYear
      );
      
      if (currentYearImages.length > 0) {
        // Find the image closest in time to the clicked image
        currentImage = currentYearImages.reduce((closest, img) => {
          const imgTime = new Date(img.time).getTime();
          const closestTime = new Date(closest.time).getTime();
          return Math.abs(imgTime - clickedTime) < Math.abs(closestTime - clickedTime) ? img : closest;
        });
      }
    }

    // Find the closest image for the previous year
    let previousImage: ImageMetadata | null = null;
    if (previousYear) {
      const previousYearImages = availableImages.filter(
        img => new Date(img.time).getFullYear() === previousYear
      );
      
      if (previousYearImages.length > 0) {
        // Find the image closest in time to the current image
        const currentTime = new Date(currentImage.time).getTime();
        previousImage = previousYearImages.reduce((closest, img) => {
          const imgTime = new Date(img.time).getTime();
          const closestTime = new Date(closest.time).getTime();
          return Math.abs(imgTime - currentTime) < Math.abs(closestTime - currentTime) ? img : closest;
        });
      }
    }

    return { previousImage, currentImage };
  }

  /**
   * Find change mask for the two images
   */
  function findChangeMask(
    previousImage: ImageMetadata,
    currentImage: ImageMetadata,
    availableMasks: ChangeMaskMetadata[]
  ): ChangeMaskMetadata | null {
    // Look for a mask that matches the image pair
    // Note: masks are stored with img_a_id < img_b_id constraint
    const mask = availableMasks.find(mask => {
      return (
        (mask.img_a_id === previousImage.id && mask.img_b_id === currentImage.id) ||
        (mask.img_a_id === currentImage.id && mask.img_b_id === previousImage.id)
      );
    });

    return mask || null;
  }

  /**
   * Load comparison data for a clicked image
   */
  async function loadComparisonData(
    clickedImage: ImageMetadata,
    availableImages: ImageMetadata[],
    availableMasks: ChangeMaskMetadata[],
    visibleYears: Set<number>
  ): Promise<void> {
    try {
      isLoading.value = true;
      hasError.value = false;
      errorMessage.value = "";

      // Find the comparison images
      const { previousImage, currentImage } = findComparisonImages(
        clickedImage,
        availableImages,
        visibleYears
      );

      if (!previousImage) {
        // Instead of throwing an error, just return early - this will be handled by the caller
        hasError.value = true;
        errorMessage.value = `Insufficient data for comparison. Need at least 2 years with images. Available years: ${Array.from(visibleYears).join(', ')}.`;
        return;
      }

      // Find change mask if available
      const changeMask = findChangeMask(previousImage, currentImage, availableMasks);

      // Load image URLs
      const [previousImageUrl, currentImageUrl] = await Promise.all([
        apiService.getImagePreviewUrl(previousImage.id),
        apiService.getImagePreviewUrl(currentImage.id),
      ]);

      // Load change mask URL if available
      let changeMaskUrl: string | null = null;
      if (changeMask) {
        try {
          changeMaskUrl = await apiService.getMaskPreviewUrl(changeMask.img_a_id, changeMask.img_b_id);
        } catch (error) {
          console.warn("Failed to load change mask preview:", error);
          // Don't fail the whole operation if mask loading fails
        }
      }

      comparisonData.value = {
        previousImage,
        currentImage,
        changeMask,
        previousImageUrl,
        currentImageUrl,
        changeMaskUrl,
      };
    } catch (error) {
      hasError.value = true;
      errorMessage.value = error instanceof Error ? error.message : "Unknown error occurred";
      console.error("Failed to load comparison data:", error);
    } finally {
      isLoading.value = false;
    }
  }

  /**
   * Clear comparison data
   */
  function clearComparisonData(): void {
    comparisonData.value = null;
    hasError.value = false;
    errorMessage.value = "";
  }

  return {
    // State
    isLoading: computed(() => isLoading.value),
    hasError: computed(() => hasError.value),
    errorMessage: computed(() => errorMessage.value),
    comparisonData: computed(() => comparisonData.value),

    // Methods
    loadComparisonData,
    clearComparisonData,
  };
}
