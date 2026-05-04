---
name: bharat-slides
description: "**Bharat-Style Consulting Slides**: Creates clean, white-space-heavy, crisp management consulting presentation slides in the Alvarez & Marsal / Bharat style. Use this skill whenever the user asks for slides that should look like a professional consulting deliverable — org design one-pagers, operating model slides, discussion documents, diagnostic proposals, or any deck where the goal is clarity, authority, and restraint rather than flash. Trigger on: 'Bharat style', 'consulting deck', 'discussion document', 'one-pager', 'operating model slide', 'clean slides', 'A&M style', or any request for minimalist, white-background, information-dense presentation slides. Also trigger when the user references this style from previous work."
---

# Bharat-Slides

A style system for creating consulting-grade presentation slides. These slides prioritize clarity, white space, and crisp wording over decoration. Every element earns its place.

This skill should be used **alongside** the core `pptx` skill — read that SKILL.md first for the mechanics of creating or editing .pptx files. This skill layers on the specific visual language and writing style.

---

## The Style in One Sentence

**White background, black and gray text, one accent color, generous margins, short declarative copy, and visible structure through numbering and thin divider lines.**

---

## Visual DNA

These are the design principles extracted from real Bharat/A&M decks. They work together as a system — cherry-picking individual elements won't produce the right feel.

### 1. Color Restraint

The palette is intentionally limited. Most of the slide is black, white, and gray. Color is a tool for emphasis, not decoration.

| Role | Value | Usage |
|------|-------|-------|
| Background | `FFFFFF` (white) | Always white. No gradients, no dark slides. |
| Primary text | `1A1A1A` (near-black) | Headlines, section headers, bold lead-ins |
| Body text | `666666` (medium gray) | Descriptions, supporting copy, sub-bullets |
| Secondary text | `555555` (darker gray) | Cadence descriptions, detail text |
| Accent | Client-specific | Section numbers, key terms, callout headers, divider lines. One color only. |
| Light accent | `F5F5F5` (off-white) | Background fill for content cards/boxes |
| Dividers | `C0C0C0` (light gray) | Thin horizontal rules between sections |
| Subtitle separator | `C0C0C0` | The ` | ` between title parts |

**Picking the accent color:** Match it to the client or context. For Columbia Sportswear work, use `007AB8` (blue). For Lululemon, use `E31837` (red). When no client context exists, default to `1A1A1A` (black) or a deep navy `1E2761`. Never use more than one accent color.

### 2. Typography

| Element | Font | Size | Weight | Color |
|---------|------|------|--------|-------|
| Slide title (bold part) | Arial Black | 18pt | Bold | `1A1A1A` |
| Slide title (light part) | Arial | 18pt | Regular | `444444` |
| Section number | Arial Black | 14pt | Bold | Accent color |
| Section header | Arial Black | 14pt | Bold | `1A1A1A` |
| Body lead-in (bold) | Arial | 11pt | Bold | `1A1A1A` |
| Body text | Arial | 10-11pt | Regular | `666666` |
| Detail / cadence text | Arial | 9-10pt | Regular | `555555` |
| Card title | Arial | 10.5pt | Bold | `1A1A1A` |
| Card subtitle | Arial | 10pt | Regular | `666666` |
| Italic callout | Arial | 11pt | Italic | `666666` |
| Page number | Arial | 8pt | Regular | `C0C0C0` |

**Key rule:** Arial Black for structural headers (slide title, section numbers). Arial for everything else. No other fonts.

### 3. Layout Grid

Standard slide is 10" x 5.63" (widescreen 16:9).

| Element | Position |
|---------|----------|
| Left margin | 0.5" (457200 EMU) |
| Right margin | 0.5" from right edge |
| Top of title | 0.25" from top |
| Title divider line | Immediately below title text |
| Content start | ~0.15" below title divider |
| Section divider lines | Full width, `C0C0C0`, 0.5pt weight |
| Bottom safe zone | Leave 0.4" clear at bottom for page number |
| Page number | Bottom-right corner, right-aligned |

### 4. Structural Patterns

These are the recurring slide structures observed across both decks. When building a new slide, start from one of these patterns.

#### Pattern A: Numbered Sections (Most Common)
The slide is divided into 2-4 numbered sections separated by thin gray horizontal lines. Each section has:
- A blue/accent number + bold black header (e.g., "1.  Chris's Role Re-Scoped")
- A one-line bold+gray description below the header
- Content blocks (cards, columns, or text) within the section

This is the workhorse layout. Use it for operating models, frameworks, and structured arguments.

#### Pattern B: Column Grid
3-5 equal columns, each with:
- A bold header (sometimes a large number in accent color above it)
- A bold subtitle
- Body text below in gray

Used for: metrics, team overviews, comparison slides, "how we prepared" summaries.

#### Pattern C: Title + Pipe + Subtitle
The slide title format: **Bold Topic** `  |  ` Light Subtitle

The pipe character is rendered in `C0C0C0` (light gray) with spaces around it. The bold part uses Arial Black in `1A1A1A`. The subtitle uses Arial in `444444`.

Example: **Creative Operating Model** `  |  ` How It Works

This format is used on nearly every content slide.

#### Pattern D: Two Connected Boxes
Two boxes (with `F5F5F5` background and a colored left-edge bar) connected by flow arrows showing handoffs. Used for showing relationships between teams or processes.

#### Pattern E: Left Label + Right Content
A bold label or section title sits left (sometimes in accent color), with detailed content to the right separated by a thin vertical line. Used for "Notes On This Section" or "Key Questions to Answer" layouts.

#### Pattern F: Section Divider
Top 60% is an image strip (edge-to-edge, often 2-3 photos side by side). Bottom 40% is white with a large bold title in accent color, left-aligned. Page number bottom-right.

### 5. Content Cards

When showing grouped information (like roles, capabilities, or deliverables), use cards:

- Background: `F5F5F5`
- No border or a thin left-edge accent bar (4px wide, in accent color or `1A1A1A`)
- Internal padding: ~0.1" on all sides
- Card title: 10.5pt bold `1A1A1A`
- Card body: 10pt regular `666666`
- Cards are arranged in rows of 2-4, with ~0.15" gaps

### 6. Divider Lines

Two types:
- **Section dividers**: Full-width horizontal lines, `C0C0C0`, 0.5pt, separating major sections
- **Title underlines**: Short underlines below column headers, `1A1A1A` or accent color, used in grid layouts

Never use decorative lines, accent lines under slide titles, or thick borders.

---

## Writing Style

The copy on these slides is as important as the visual design. Bharat-style writing is:

### Crisp, Not Clever
- Lead with the point, not the setup
- Every bullet should be readable in under 5 seconds
- No jargon for jargon's sake ("synergize", "leverage", "unlock" are red flags unless they're the most precise word)

### Declarative
- "Chris reports to Matt" not "It is proposed that Chris's reporting line be adjusted"
- "Joe doesn't need a reporting line" not "There may not be a need for a formal reporting relationship"
- Start bullets with the subject or action, not with "The" or "In order to"

### Structured
- Use numbered sections (1. 2. 3.) for the main argument flow
- Use short bold lead-ins for each bullet: **"Brand Identity"** Guidelines & design system
- Use the em-dash (—) to connect cause and effect within a single line

### Appropriately Sparse
- A slide with 3 well-chosen points beats one with 8 shallow ones
- If a bullet is longer than 2 lines on the slide, it needs to be split or cut
- Italic callout lines at section footers for human, non-corporate emphasis (e.g., "Small team: Ethan Watson + 2 focused on building the brand toolkit")

### Title Conventions
- Content slides: **Bold Topic  |  Light Subtitle** format
- Section numbers in accent color, flush with the left margin
- Sentence case for descriptions, Title Case for headers

---

## Applying This Skill

When you get a request that calls for this style:

1. **Read the pptx SKILL.md first** for the mechanics (PptxGenJS for new slides, unpack/edit/pack for existing)
2. **Pick the accent color** based on the client context
3. **Choose a structural pattern** (A-F above) based on the content type
4. **Write the copy first** before building the slide — get the wording crisp and short
5. **Build the slide** using the exact fonts, sizes, and colors in the tables above
6. **QA check** for white space, alignment, and copy length — if anything feels crowded, remove content rather than shrinking fonts

### Common Mistakes to Avoid

- Adding color backgrounds to slides (always white)
- Using more than one accent color
- Making text too small to fit more content (cut content instead)
- Forgetting the thin gray divider lines between sections
- Using rounded corners on boxes (keep everything rectangular)
- Adding drop shadows, gradients, or visual effects
- Writing in full sentences when a short phrase will do
- Centering body text (left-align everything except slide titles)

---

## Quick Reference: PptxGenJS Color Map

When building slides programmatically, use these hex values (without the #):

```javascript
const BHARAT = {
  white:      'FFFFFF',
  black:      '1A1A1A',
  gray:       '666666',
  darkGray:   '555555',
  subtitle:   '444444',
  lightGray:  'C0C0C0',
  cardBg:     'F5F5F5',
  // Set per client:
  accent:     '007AB8', // Columbia blue (or E31837 for Lulu red, etc.)
};
```
