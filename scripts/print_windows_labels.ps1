param(
    [Parameter(Mandatory = $true)]
    [string]$PayloadPath
)

$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing

function Draw-FitImage {
    param(
        [System.Drawing.Graphics]$Graphics,
        [System.Drawing.Image]$Image,
        [float]$X,
        [float]$Y,
        [float]$Width,
        [float]$Height
    )
    if ($Image.Width -le 0 -or $Image.Height -le 0) { return }
    $scale = [Math]::Min($Width / $Image.Width, $Height / $Image.Height)
    $drawWidth = $Image.Width * $scale
    $drawHeight = $Image.Height * $scale
    $drawX = $X + (($Width - $drawWidth) / 2)
    $drawY = $Y + (($Height - $drawHeight) / 2)
    $Graphics.DrawImage($Image, $drawX, $drawY, $drawWidth, $drawHeight)
}

function Draw-FitImageLightened {
    param(
        [System.Drawing.Graphics]$Graphics,
        [System.Drawing.Image]$Image,
        [float]$X,
        [float]$Y,
        [float]$Width,
        [float]$Height,
        [System.Drawing.Imaging.ColorMatrix]$ColorMatrix
    )
    if ($Image.Width -le 0 -or $Image.Height -le 0) { return }
    $scale = [Math]::Min($Width / $Image.Width, $Height / $Image.Height)
    $drawWidth = $Image.Width * $scale
    $drawHeight = $Image.Height * $scale
    $drawX = $X + (($Width - $drawWidth) / 2)
    $drawY = $Y + (($Height - $drawHeight) / 2)
    $destRect = New-Object System.Drawing.Rectangle(
        [int][Math]::Round($drawX),
        [int][Math]::Round($drawY),
        [int][Math]::Round($drawWidth),
        [int][Math]::Round($drawHeight)
    )
    $attributes = New-Object System.Drawing.Imaging.ImageAttributes
    try {
        $attributes.SetColorMatrix($ColorMatrix)
        $Graphics.DrawImage(
            $Image,
            $destRect,
            0,
            0,
            $Image.Width,
            $Image.Height,
            [System.Drawing.GraphicsUnit]::Pixel,
            $attributes
        )
    } finally {
        $attributes.Dispose()
    }
}

function Draw-TextFit {
    param(
        [System.Drawing.Graphics]$Graphics,
        [string]$Text,
        [System.Drawing.Font]$Font,
        [System.Drawing.Brush]$Brush,
        [float]$X,
        [float]$Y,
        [float]$Width,
        [float]$Height,
        [System.Drawing.StringFormat]$Format
    )
    $Graphics.DrawString($Text, $Font, $Brush, [System.Drawing.RectangleF]::new($X, $Y, $Width, $Height), $Format)
}

function Draw-ShrinkingText {
    param(
        [System.Drawing.Graphics]$Graphics,
        [string]$Text,
        [string]$FontFamily,
        [System.Drawing.FontStyle]$Style,
        [float]$BaseSize,
        [float]$MinSize,
        [System.Drawing.Brush]$Brush,
        [float]$X,
        [float]$Y,
        [float]$Width,
        [float]$Height,
        [System.Drawing.StringFormat]$Format
    )
    $size = $BaseSize
    while ($size -ge $MinSize) {
        $font = New-Object System.Drawing.Font($FontFamily, $size, $Style)
        try {
            $measured = $Graphics.MeasureString($Text, $font, 10000, $Format)
            if ($measured.Width -le ($Width + 0.5) -and $measured.Height -le ($Height + 2)) {
                $Graphics.DrawString($Text, $font, $Brush, [System.Drawing.RectangleF]::new($X, $Y, $Width, $Height), $Format)
                return
            }
        } finally {
            $font.Dispose()
        }
        $size -= 0.5
    }

    $fallbackFont = New-Object System.Drawing.Font($FontFamily, $MinSize, $Style)
    try {
        $Graphics.DrawString($Text, $fallbackFont, $Brush, [System.Drawing.RectangleF]::new($X, $Y, $Width, $Height), $Format)
    } finally {
        $fallbackFont.Dispose()
    }
}

function Get-EmployeeLineLabel {
    param($Line)
    switch ([string]$Line.field) {
        "name" { "ITEM" }
        "set" { "SET" }
        "condition" { "COND" }
        "location" { "LOC" }
        default {
            $fallback = ([string]$Line.label).ToUpperInvariant()
            if ($fallback.Length -gt 6) { $fallback.Substring(0, 6) } else { $fallback }
        }
    }
}

$payload = Get-Content -Raw -Path $PayloadPath | ConvertFrom-Json
$labels = @($payload.labels)
if ($labels.Count -eq 0) { exit 0 }

$state = [pscustomobject]@{
    Index = 0
    Labels = $labels
}

$doc = New-Object System.Drawing.Printing.PrintDocument
$doc.PrinterSettings.PrinterName = [string]$payload.printer_name
if (-not $doc.PrinterSettings.IsValid) {
    throw "Printer is not valid: $($payload.printer_name)"
}
$doc.DocumentName = "Degen 3x1 Inventory Labels"
$doc.PrintController = New-Object System.Drawing.Printing.StandardPrintController
$doc.DefaultPageSettings.PaperSize = New-Object System.Drawing.Printing.PaperSize(
    "Degen 3x1 Label",
    [int]$payload.paper_width_hundredths,
    [int]$payload.paper_height_hundredths
)
$doc.DefaultPageSettings.Margins = New-Object System.Drawing.Printing.Margins(0, 0, 0, 0)
$doc.DefaultPageSettings.Landscape = $false

$black = [System.Drawing.Brushes]::Black
$borderPen = New-Object System.Drawing.Pen([System.Drawing.Color]::Black, 1)
$foldPen = New-Object System.Drawing.Pen([System.Drawing.Color]::Gray, 1)
$foldPen.DashStyle = [System.Drawing.Drawing2D.DashStyle]::Dash
$center = New-Object System.Drawing.StringFormat
$center.Alignment = [System.Drawing.StringAlignment]::Center
$center.LineAlignment = [System.Drawing.StringAlignment]::Center
$center.FormatFlags = [System.Drawing.StringFormatFlags]::NoWrap
$left = New-Object System.Drawing.StringFormat
$left.Alignment = [System.Drawing.StringAlignment]::Near
$left.LineAlignment = [System.Drawing.StringAlignment]::Near
$left.FormatFlags = [System.Drawing.StringFormatFlags]::NoWrap
$left.Trimming = [System.Drawing.StringTrimming]::EllipsisCharacter
$priceFormat = New-Object System.Drawing.StringFormat
$priceFormat.Alignment = [System.Drawing.StringAlignment]::Center
$priceFormat.LineAlignment = [System.Drawing.StringAlignment]::Center
$priceFormat.FormatFlags = [System.Drawing.StringFormatFlags]::NoWrap
$priceFormat.Trimming = [System.Drawing.StringTrimming]::None

$fontLine = New-Object System.Drawing.Font("Arial", 7, [System.Drawing.FontStyle]::Bold)
$fontLabel = New-Object System.Drawing.Font("Arial", 7, [System.Drawing.FontStyle]::Bold)
$fontCode = New-Object System.Drawing.Font("Consolas", 6.5, [System.Drawing.FontStyle]::Bold)
$logoLightenMatrix = New-Object System.Drawing.Imaging.ColorMatrix
$logoLightenMatrix.Matrix00 = 0.45
$logoLightenMatrix.Matrix11 = 0.45
$logoLightenMatrix.Matrix22 = 0.45
$logoLightenMatrix.Matrix33 = 1
$logoLightenMatrix.Matrix44 = 1
$logoLightenMatrix.Matrix40 = 0.42
$logoLightenMatrix.Matrix41 = 0.42
$logoLightenMatrix.Matrix42 = 0.42

$logoImage = $null
if ($payload.logo_path -and (Test-Path ([string]$payload.logo_path))) {
    $logoImage = [System.Drawing.Image]::FromFile([string]$payload.logo_path)
}

$handler = [System.Drawing.Printing.PrintPageEventHandler]{
    param($sender, $e)

    $label = $state.Labels[$state.Index]
    $g = $e.Graphics
    $g.PageUnit = [System.Drawing.GraphicsUnit]::Display
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
    $g.TextRenderingHint = [System.Drawing.Text.TextRenderingHint]::SingleBitPerPixelGridFit

    $g.DrawRectangle($borderPen, 2, 2, 296, 96)
    $g.DrawLine($foldPen, 102, 4, 102, 96)
    $g.DrawLine($foldPen, 106, 4, 106, 96)

    if ($logoImage) {
        Draw-FitImageLightened -Graphics $g -Image $logoImage -X 16 -Y 7 -Width 74 -Height 50 -ColorMatrix $logoLightenMatrix
    }

    $priceBaseSize = 21
    if ($label.price_class -eq "price-medium") {
        $priceBaseSize = 18
    } elseif ($label.price_class -eq "price-long") {
        $priceBaseSize = 12
    }
    Draw-ShrinkingText -Graphics $g -Text ([string]$label.price_text) -FontFamily "Arial" -Style ([System.Drawing.FontStyle]::Bold) -BaseSize $priceBaseSize -MinSize 10 -Brush $black -X 5 -Y 57 -Width 94 -Height 36 -Format $priceFormat

    if ($label.barcode_image_path -and (Test-Path ([string]$label.barcode_image_path))) {
        $barcodeImage = [System.Drawing.Image]::FromFile([string]$label.barcode_image_path)
        try {
            Draw-FitImage -Graphics $g -Image $barcodeImage -X 128 -Y 6 -Width 112 -Height 32
        } finally {
            $barcodeImage.Dispose()
        }
    }
    Draw-TextFit -Graphics $g -Text ([string]$label.barcode_value) -Font $fontCode -Brush $black -X 128 -Y 38 -Width 112 -Height 10 -Format $center

    $lineY = 52
    foreach ($line in @($label.employee_lines)) {
        if ($line.field -eq "barcode") { continue }
        if ($lineY -gt 88) { break }
        $lineLabel = Get-EmployeeLineLabel -Line $line
        Draw-TextFit -Graphics $g -Text $lineLabel -Font $fontLabel -Brush $black -X 112 -Y $lineY -Width 34 -Height 10 -Format $left
        Draw-TextFit -Graphics $g -Text ([string]$line.value) -Font $fontLine -Brush $black -X 148 -Y $lineY -Width 144 -Height 10 -Format $left
        $lineY += 10
    }

    $state.Index += 1
    $e.HasMorePages = $state.Index -lt $state.Labels.Count
}

try {
    $doc.add_PrintPage($handler)
    $doc.Print()
} finally {
    if ($logoImage) { $logoImage.Dispose() }
    $fontLine.Dispose()
    $fontLabel.Dispose()
    $fontCode.Dispose()
    $borderPen.Dispose()
    $foldPen.Dispose()
    $center.Dispose()
    $left.Dispose()
    $priceFormat.Dispose()
    $doc.Dispose()
}
