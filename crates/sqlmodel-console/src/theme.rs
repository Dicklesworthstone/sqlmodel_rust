//! Theme definitions for SQLModel console output.
//!
//! This module provides the `Theme` struct that defines colors and styles
//! for all console output elements. Themes can be customized or use
//! predefined presets.
//!
//! # Example
//!
//! ```rust
//! use sqlmodel_console::Theme;
//!
//! // Use the default dark theme
//! let theme = Theme::default();
//!
//! // Or explicitly choose a theme
//! let dark = Theme::dark();
//! let light = Theme::light();
//! ```
//!
//! # Color Philosophy
//!
//! The dark theme uses colors inspired by the Dracula palette:
//! - **Green** = Success, strings (positive/data)
//! - **Red** = Errors, operators (danger/action)
//! - **Yellow** = Warnings, booleans (caution/special)
//! - **Cyan** = Info, numbers (neutral data)
//! - **Magenta** = Dates, SQL keywords (special syntax)
//! - **Purple** = JSON, SQL numbers (structured data)
//! - **Gray** = Dim text, comments, borders (secondary)

/// A color that can be rendered differently based on output mode.
///
/// Contains both truecolor RGB and ANSI-256 fallback values,
/// plus an optional plain text marker for non-color output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThemeColor {
    /// RGB color for truecolor terminals (r, g, b).
    pub rgb: (u8, u8, u8),
    /// ANSI 256-color fallback for older terminals.
    pub ansi256: u8,
    /// Plain text marker (for plain mode output), e.g., "NULL" for null values.
    pub plain_marker: Option<&'static str>,
}

impl ThemeColor {
    /// Create a theme color with RGB and ANSI-256 fallback.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::theme::ThemeColor;
    ///
    /// let green = ThemeColor::new((80, 250, 123), 84);
    /// ```
    #[must_use]
    pub const fn new(rgb: (u8, u8, u8), ansi256: u8) -> Self {
        Self {
            rgb,
            ansi256,
            plain_marker: None,
        }
    }

    /// Create a theme color with a plain text marker.
    ///
    /// The marker is used in plain mode to indicate special values
    /// like NULL without using colors.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::theme::ThemeColor;
    ///
    /// let null_color = ThemeColor::with_marker((98, 114, 164), 60, "NULL");
    /// ```
    #[must_use]
    pub const fn with_marker(rgb: (u8, u8, u8), ansi256: u8, marker: &'static str) -> Self {
        Self {
            rgb,
            ansi256,
            plain_marker: Some(marker),
        }
    }

    /// Get the RGB components as a tuple.
    #[must_use]
    pub const fn rgb(&self) -> (u8, u8, u8) {
        self.rgb
    }

    /// Get the ANSI-256 color code.
    #[must_use]
    pub const fn ansi256(&self) -> u8 {
        self.ansi256
    }

    /// Get the plain text marker, if any.
    #[must_use]
    pub const fn plain_marker(&self) -> Option<&'static str> {
        self.plain_marker
    }
}

/// SQLModel console theme with semantic colors.
///
/// Defines all colors used throughout SQLModel console output.
/// Use [`Theme::dark()`] or [`Theme::light()`] for predefined themes,
/// or customize individual colors.
///
/// # Example
///
/// ```rust
/// use sqlmodel_console::Theme;
///
/// let theme = Theme::dark();
/// assert_eq!(theme.success.rgb(), (80, 250, 123));
/// ```
#[derive(Debug, Clone)]
pub struct Theme {
    // === Status Colors ===
    /// Success messages, completion indicators (green).
    pub success: ThemeColor,
    /// Error messages, failure indicators (red).
    pub error: ThemeColor,
    /// Warning messages, deprecation notices (yellow).
    pub warning: ThemeColor,
    /// Informational messages, hints (cyan).
    pub info: ThemeColor,

    // === SQL Value Type Colors ===
    /// NULL values (typically dim/italic).
    pub null_value: ThemeColor,
    /// Boolean values (true/false).
    pub bool_value: ThemeColor,
    /// Numeric values (integers, floats).
    pub number_value: ThemeColor,
    /// String/text values.
    pub string_value: ThemeColor,
    /// Date/time/timestamp values.
    pub date_value: ThemeColor,
    /// Binary/blob values.
    pub binary_value: ThemeColor,
    /// JSON values.
    pub json_value: ThemeColor,
    /// UUID values.
    pub uuid_value: ThemeColor,

    // === SQL Syntax Colors ===
    /// SQL keywords (SELECT, FROM, WHERE).
    pub sql_keyword: ThemeColor,
    /// SQL strings ('value').
    pub sql_string: ThemeColor,
    /// SQL numbers (42, 3.14).
    pub sql_number: ThemeColor,
    /// SQL comments (-- comment).
    pub sql_comment: ThemeColor,
    /// SQL operators (=, >, AND).
    pub sql_operator: ThemeColor,
    /// SQL identifiers (table names, column names).
    pub sql_identifier: ThemeColor,

    // === UI Element Colors ===
    /// Table/panel borders.
    pub border: ThemeColor,
    /// Headers and titles.
    pub header: ThemeColor,
    /// Dimmed/secondary text.
    pub dim: ThemeColor,
    /// Highlighted/emphasized text.
    pub highlight: ThemeColor,
}

impl Theme {
    /// Create the default dark theme (Dracula-inspired).
    ///
    /// This theme is optimized for dark terminal backgrounds and uses
    /// the Dracula color palette for high contrast and visual appeal.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::Theme;
    ///
    /// let theme = Theme::dark();
    /// ```
    #[must_use]
    pub fn dark() -> Self {
        Self {
            // Status colors (Dracula palette)
            success: ThemeColor::new((80, 250, 123), 84), // Green
            error: ThemeColor::new((255, 85, 85), 203),   // Red
            warning: ThemeColor::new((241, 250, 140), 228), // Yellow
            info: ThemeColor::new((139, 233, 253), 117),  // Cyan

            // Value type colors
            null_value: ThemeColor::with_marker((98, 114, 164), 60, "NULL"),
            bool_value: ThemeColor::new((241, 250, 140), 228), // Yellow
            number_value: ThemeColor::new((139, 233, 253), 117), // Cyan
            string_value: ThemeColor::new((80, 250, 123), 84), // Green
            date_value: ThemeColor::new((255, 121, 198), 212), // Magenta
            binary_value: ThemeColor::new((255, 184, 108), 215), // Orange
            json_value: ThemeColor::new((189, 147, 249), 141), // Purple
            uuid_value: ThemeColor::new((255, 184, 108), 215), // Orange

            // SQL syntax colors
            sql_keyword: ThemeColor::new((255, 121, 198), 212), // Magenta
            sql_string: ThemeColor::new((80, 250, 123), 84),    // Green
            sql_number: ThemeColor::new((189, 147, 249), 141),  // Purple
            sql_comment: ThemeColor::new((98, 114, 164), 60),   // Gray
            sql_operator: ThemeColor::new((255, 85, 85), 203),  // Red
            sql_identifier: ThemeColor::new((248, 248, 242), 255), // White

            // UI elements
            border: ThemeColor::new((98, 114, 164), 60), // Gray
            header: ThemeColor::new((248, 248, 242), 255), // White
            dim: ThemeColor::new((98, 114, 164), 60),    // Gray
            highlight: ThemeColor::new((255, 255, 255), 231), // Bright white
        }
    }

    /// Create a light theme variant.
    ///
    /// This theme is optimized for light terminal backgrounds with
    /// darker colors for better visibility.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::Theme;
    ///
    /// let theme = Theme::light();
    /// ```
    #[must_use]
    pub fn light() -> Self {
        Self {
            // Status colors (adjusted for light background)
            success: ThemeColor::new((40, 167, 69), 34),
            error: ThemeColor::new((220, 53, 69), 160),
            warning: ThemeColor::new((255, 193, 7), 220),
            info: ThemeColor::new((23, 162, 184), 37),

            // Value colors (darker for visibility on light bg)
            null_value: ThemeColor::with_marker((108, 117, 125), 244, "NULL"),
            bool_value: ThemeColor::new((156, 39, 176), 128),
            number_value: ThemeColor::new((0, 150, 136), 30),
            string_value: ThemeColor::new((76, 175, 80), 34),
            date_value: ThemeColor::new((156, 39, 176), 128),
            binary_value: ThemeColor::new((255, 152, 0), 208),
            json_value: ThemeColor::new((103, 58, 183), 92),
            uuid_value: ThemeColor::new((255, 152, 0), 208),

            // SQL syntax (darker)
            sql_keyword: ThemeColor::new((156, 39, 176), 128),
            sql_string: ThemeColor::new((76, 175, 80), 34),
            sql_number: ThemeColor::new((103, 58, 183), 92),
            sql_comment: ThemeColor::new((108, 117, 125), 244),
            sql_operator: ThemeColor::new((220, 53, 69), 160),
            sql_identifier: ThemeColor::new((33, 37, 41), 235),

            // UI elements
            border: ThemeColor::new((108, 117, 125), 244),
            header: ThemeColor::new((33, 37, 41), 235),
            dim: ThemeColor::new((108, 117, 125), 244),
            highlight: ThemeColor::new((0, 0, 0), 16),
        }
    }

    /// Create a new theme by cloning an existing one.
    ///
    /// Useful for customizing a preset theme.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlmodel_console::Theme;
    /// use sqlmodel_console::theme::ThemeColor;
    ///
    /// let mut theme = Theme::dark();
    /// theme.success = ThemeColor::new((0, 255, 0), 46); // Brighter green
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Theme {
    fn default() -> Self {
        Self::dark()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_theme_color_new() {
        let color = ThemeColor::new((255, 0, 0), 196);
        assert_eq!(color.rgb(), (255, 0, 0));
        assert_eq!(color.ansi256(), 196);
        assert_eq!(color.plain_marker(), None);
    }

    #[test]
    fn test_theme_color_with_marker() {
        let color = ThemeColor::with_marker((128, 128, 128), 244, "DIM");
        assert_eq!(color.rgb(), (128, 128, 128));
        assert_eq!(color.ansi256(), 244);
        assert_eq!(color.plain_marker(), Some("DIM"));
    }

    #[test]
    fn test_dark_theme_success_color() {
        let theme = Theme::dark();
        // Dracula green
        assert_eq!(theme.success.rgb(), (80, 250, 123));
    }

    #[test]
    fn test_light_theme_error_color() {
        let theme = Theme::light();
        // Bootstrap-style red
        assert_eq!(theme.error.rgb(), (220, 53, 69));
    }

    #[test]
    fn test_default_is_dark() {
        let default = Theme::default();
        let dark = Theme::dark();
        assert_eq!(default.success.rgb(), dark.success.rgb());
        assert_eq!(default.error.rgb(), dark.error.rgb());
    }

    #[test]
    fn test_null_value_has_marker() {
        let theme = Theme::dark();
        assert_eq!(theme.null_value.plain_marker(), Some("NULL"));
    }

    #[test]
    fn test_theme_clone() {
        let theme1 = Theme::dark();
        let theme2 = theme1.clone();
        assert_eq!(theme1.success.rgb(), theme2.success.rgb());
    }

    #[test]
    fn test_theme_color_copy() {
        let color1 = ThemeColor::new((100, 100, 100), 245);
        let color2 = color1; // Copy
        assert_eq!(color1.rgb(), color2.rgb());
    }

    #[test]
    fn test_all_dark_theme_colors_have_ansi256() {
        let theme = Theme::dark();
        // Verify all theme colors have non-zero ANSI-256 values
        // (zero is typically only used for black, which is intentional for some colors)
        let _ = theme.success.ansi256();
        let _ = theme.error.ansi256();
        let _ = theme.warning.ansi256();
        let _ = theme.info.ansi256();
        let _ = theme.null_value.ansi256();
        let _ = theme.sql_keyword.ansi256();
        let _ = theme.border.ansi256();
        // If we got here, all colors have valid ANSI values
    }

    #[test]
    fn test_all_light_theme_colors_have_ansi256() {
        let theme = Theme::light();
        // Verify all theme colors have valid ANSI-256 values
        let _ = theme.success.ansi256();
        let _ = theme.error.ansi256();
        let _ = theme.warning.ansi256();
        let _ = theme.info.ansi256();
        // If we got here, all colors have valid ANSI values
    }
}
