# WTree3 Documentation

This directory contains comprehensive documentation for the WTree3 library.

## Documentation Files

### 📖 User Documentation

| File | Description | Audience |
|------|-------------|----------|
| [**EXAMPLES.md**](EXAMPLES.md) | 50+ copy-paste ready code examples organized by topic | Developers |
| [**DOCUMENTATION.md**](DOCUMENTATION.md) | Complete guide with architecture, patterns, and best practices | All users |
| [**DOCUMENTATION_SUMMARY.md**](DOCUMENTATION_SUMMARY.md) | Quick overview and getting started guide | New users |

### 🔧 Generated Documentation

| Directory | Description | How to Generate |
|-----------|-------------|-----------------|
| **api/** | Doxygen-generated API reference (HTML) | `doxygen Doxyfile` from project root |

**Note**: The `api/` directory is git-ignored as it contains generated files.

## Quick Navigation

### For New Users

1. Start with the [Quick Start example](EXAMPLES.md#quick-start)
2. Browse [common patterns](DOCUMENTATION.md#common-patterns)
3. Read [architecture overview](DOCUMENTATION.md#architecture)

### For Integration

1. Review [API Reference](api/html/index.html) (after generating)
2. Study [batch operations examples](EXAMPLES.md#batch-operations)
3. Check [error handling patterns](DOCUMENTATION.md#error-handling-best-practices)

### For Performance Tuning

1. Read [performance tips](EXAMPLES.md#performance-tips)
2. Study [memory optimization](EXAMPLES.md#memory-optimization)
3. Review [performance tuning guide](DOCUMENTATION.md#performance-tuning)

## Generating API Documentation

### Prerequisites

Install Doxygen and Graphviz (for diagrams):

```bash
# Ubuntu/Debian
sudo apt-get install doxygen graphviz

# macOS
brew install doxygen graphviz

# Windows
# Download from: https://www.doxygen.nl/download.html
#                https://graphviz.org/download/
```

### Generate Documentation

From the project root:

```bash
# Generate HTML documentation
doxygen Doxyfile

# Output will be in: docs/api/html/index.html
```

### View Documentation

```bash
# macOS
open docs/api/html/index.html

# Linux
xdg-open docs/api/html/index.html

# Windows
start docs\api\html\index.html
```

## Documentation Structure

```
docs/
├── README.md                   # This file
├── EXAMPLES.md                 # Code examples (50+ patterns)
├── DOCUMENTATION.md            # Complete user guide
├── DOCUMENTATION_SUMMARY.md    # Quick overview
└── api/                        # Generated API docs (git-ignored)
    ├── html/
    │   ├── index.html         # Main entry point
    │   ├── files.html         # File list
    │   ├── globals.html       # Function index
    │   └── ...
    └── latex/                  # LaTeX output (optional)
```

## Customizing Doxygen Output

The main Doxyfile is in the project root. Key settings for WTree3:

```ini
PROJECT_NAME           = "WTree3"
PROJECT_NUMBER         = "3.0"
OUTPUT_DIRECTORY       = docs/api
INPUT                  = src/wtree3.h README.md
OPTIMIZE_OUTPUT_FOR_C  = YES
EXTRACT_ALL            = YES
SOURCE_BROWSER         = YES
GENERATE_TREEVIEW      = YES
HAVE_DOT               = YES
CALL_GRAPH             = YES
```

See `Doxyfile.wtree3` in the project root for recommended settings.

## Building PDF Documentation (Optional)

If you want PDF output:

1. Enable LaTeX generation in Doxyfile:
   ```ini
   GENERATE_LATEX         = YES
   ```

2. Generate and build:
   ```bash
   doxygen Doxyfile
   cd docs/api/latex
   make
   # Output: refman.pdf
   ```

## Contributing to Documentation

### Adding Examples

Add new examples to [EXAMPLES.md](EXAMPLES.md) following the existing format:

```markdown
### Example Title

```c
// Well-commented, compilable code
#include "wtree3.h"

int main() {
    // ...
}
```

Explanation of what the example demonstrates.
```

### Updating API Documentation

API documentation lives in the source code (src/wtree3.h) as Doxygen comments:

```c
/**
 * @brief Short description
 *
 * Detailed description with multiple paragraphs.
 *
 * @param param1 Description of parameter 1
 * @param param2 Description of parameter 2
 * @return Description of return value
 *
 * @par Example:
 * @code{.c}
 * // Example code here
 * @endcode
 */
int wtree3_function(int param1, int param2);
```

After editing, regenerate with `doxygen Doxyfile`.

### Updating Guides

Edit [DOCUMENTATION.md](DOCUMENTATION.md) for:
- Architecture changes
- New best practices
- Performance tips
- Common patterns

## Documentation Quality Checklist

When updating documentation:

- [ ] Code examples compile without errors
- [ ] All public functions documented
- [ ] Examples include error handling
- [ ] Memory management is clear
- [ ] Thread safety is documented
- [ ] Performance implications mentioned
- [ ] Cross-references are correct
- [ ] Doxygen generates without warnings

## Support and Questions

- **Issues**: https://github.com/yourusername/wtree/issues
- **Discussions**: https://github.com/yourusername/wtree/discussions
- **Documentation bugs**: File an issue with label "documentation"

## Documentation Versions

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-01-10 | Initial comprehensive documentation |

---

**Last Updated**: 2026-01-10
**WTree3 Version**: 3.0
