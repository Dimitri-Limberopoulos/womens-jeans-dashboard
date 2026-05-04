/**
 * Bharat-Slides: PptxGenJS Starter Template
 *
 * Copy and adapt this when building new slides in the Bharat style.
 * This file shows the common patterns — title bar, numbered sections,
 * content cards, divider lines, and page numbers.
 */

const PptxGenJS = require('pptxgenjs');
const pptx = new PptxGenJS();

// ─── CONSTANTS ───────────────────────────────────────────────
const COLORS = {
  white:     'FFFFFF',
  black:     '1A1A1A',
  gray:      '666666',
  darkGray:  '555555',
  subtitle:  '444444',
  lightGray: 'C0C0C0',
  cardBg:    'F5F5F5',
  accent:    '007AB8', // ← Change per client
};

const FONTS = {
  header: 'Arial Black',
  body:   'Arial',
};

// Slide dimensions (inches)
const MARGIN_LEFT = 0.5;
const MARGIN_RIGHT = 0.5;
const CONTENT_WIDTH = 9.0; // 10 - 0.5 - 0.5
const SLIDE_WIDTH = 10;

// ─── HELPERS ─────────────────────────────────────────────────

/** Add the standard title bar: Bold Topic  |  Light Subtitle */
function addTitle(slide, boldPart, lightPart, y = 0.25) {
  slide.addText([
    { text: boldPart, options: {
      fontFace: FONTS.header, fontSize: 18, bold: true, color: COLORS.black
    }},
    { text: '  |  ', options: {
      fontFace: FONTS.body, fontSize: 18, color: COLORS.lightGray
    }},
    { text: lightPart, options: {
      fontFace: FONTS.body, fontSize: 18, color: COLORS.subtitle
    }},
  ], { x: MARGIN_LEFT, y: y, w: CONTENT_WIDTH, h: 0.4, margin: 0 });
}

/** Add a full-width thin gray divider line */
function addDivider(slide, y) {
  slide.addShape(pptx.ShapeType.line, {
    x: MARGIN_LEFT, y: y, w: CONTENT_WIDTH, h: 0,
    line: { color: COLORS.lightGray, width: 0.5 },
  });
}

/** Add a numbered section header: "1.  Section Name" */
function addSectionHeader(slide, number, text, y) {
  slide.addText([
    { text: `${number}.  `, options: {
      fontFace: FONTS.header, fontSize: 14, bold: true, color: COLORS.accent
    }},
    { text: text, options: {
      fontFace: FONTS.header, fontSize: 14, bold: true, color: COLORS.black
    }},
  ], { x: MARGIN_LEFT, y: y, w: CONTENT_WIDTH, h: 0.28, margin: 0 });
}

/** Add body text with optional bold lead-in */
function addBody(slide, boldLead, grayText, x, y, w) {
  const parts = [];
  if (boldLead) {
    parts.push({ text: boldLead + ' ', options: {
      fontFace: FONTS.body, fontSize: 11, bold: true, color: COLORS.black
    }});
  }
  parts.push({ text: grayText, options: {
    fontFace: FONTS.body, fontSize: 11, color: COLORS.gray
  }});
  slide.addText(parts, { x: x, y: y, w: w, h: 0.22, margin: 0 });
}

/** Add a content card (F5F5F5 box with optional accent left bar) */
function addCard(slide, x, y, w, h, options = {}) {
  // Card background
  slide.addShape(pptx.ShapeType.rect, {
    x: x, y: y, w: w, h: h,
    fill: { color: COLORS.cardBg },
  });
  // Optional accent left bar
  if (options.accentBar) {
    slide.addShape(pptx.ShapeType.rect, {
      x: x, y: y, w: 0.05, h: h,
      fill: { color: options.accentColor || COLORS.accent },
    });
  }
}

/** Add page number in bottom-right */
function addPageNumber(slide, num) {
  slide.addText(String(num), {
    x: 9.2, y: 5.25, w: 0.4, h: 0.2,
    fontFace: FONTS.body, fontSize: 8, color: COLORS.lightGray,
    align: 'right', margin: 0,
  });
}

// ─── EXAMPLE: BUILD A SLIDE ──────────────────────────────────

const slide = pptx.addSlide();

// Title
addTitle(slide, 'Creative Operating Model', 'How It Works');
addDivider(slide, 0.7);

// Section 1
addSectionHeader(slide, 1, "Chris's Role Re-Scoped", 0.9);
addBody(slide, 'Chris reports to Matt.', 'His remit is narrowed and specific:', MARGIN_LEFT, 1.22, CONTENT_WIDTH);

// Cards row
const cardW = 2.12;
const cardGap = 0.15;
const cardY = 1.5;
const labels = [
  ['Brand Identity', 'Guidelines & design system'],
  ['Campaign Ideation', 'Brand campaign development'],
  ['Art Direction', 'Seasonal creative strategy'],
  ['Innovation', 'Creative exploration (e.g. tech)'],
];
labels.forEach((pair, i) => {
  const cx = MARGIN_LEFT + i * (cardW + cardGap);
  addCard(slide, cx, cardY, cardW, 0.5);
  slide.addText([
    { text: pair[0], options: { fontFace: FONTS.body, fontSize: 10.5, bold: true, color: COLORS.black }},
    { text: '\n' + pair[1], options: { fontFace: FONTS.body, fontSize: 10, color: COLORS.gray }},
  ], { x: cx + 0.1, y: cardY + 0.05, w: cardW - 0.2, h: 0.4, margin: 0, valign: 'middle' });
});

// Italic callout
slide.addText('Small team: Ethan Watson + 2 focused on building the brand toolkit', {
  x: MARGIN_LEFT, y: 2.06, w: CONTENT_WIDTH, h: 0.18,
  fontFace: FONTS.body, fontSize: 11, italic: true, color: COLORS.gray, margin: 0,
});

// Section divider
addDivider(slide, 2.32);

// Section 2...
addSectionHeader(slide, 2, 'Creative Becomes Two Teams, Tightly Connected', 2.41);

// Page number
addPageNumber(slide, 1);

// ─── EXPORT ──────────────────────────────────────────────────
pptx.writeFile({ fileName: 'bharat-style-example.pptx' })
  .then(() => console.log('Done'))
  .catch(err => console.error(err));
