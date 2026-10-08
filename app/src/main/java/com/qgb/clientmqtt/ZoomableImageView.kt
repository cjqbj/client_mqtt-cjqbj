package com.qgb.clientmqtt

import android.content.Context
import android.graphics.Matrix
import android.graphics.RectF
import android.graphics.drawable.Drawable
import android.util.AttributeSet
import android.view.GestureDetector
import android.view.MotionEvent
import android.view.ScaleGestureDetector
import android.widget.ImageView

/**
 * 可双指缩放 / 拖动 / 双击还原的图片控件，供 Python feature 通过
 * `pyui_kit.make_zoom_image(context)` 实例化（com.qgb.clientmqtt.ZoomableImageView）。
 *
 * 约定：控件对宿主容器友好——未放大时把滑动事件交还给 ScrollView/Pager，
 * 放大后才请求父容器不拦截，保证图片可拖动且页面仍可滚动。
 */
class ZoomableImageView @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null
) : ImageView(context, attrs) {

    private val baseMatrix = Matrix()
    private val workMatrix = Matrix()
    private var fitScale = 1f
    private var currentScale = 1f
    private val maxScaleMultiple = 6f
    private var scaling = false
    private val values = FloatArray(9)

    /** 单击（未放大状态下）回调，feature 可借此点开大图等；默认空。 */
    var onImageClick: (() -> Unit)? = null

    private val scaleDetector =
        ScaleGestureDetector(context, object : ScaleGestureDetector.SimpleOnScaleGestureListener() {
            override fun onScaleBegin(detector: ScaleGestureDetector): Boolean {
                scaling = true
                return true
            }

            override fun onScale(detector: ScaleGestureDetector): Boolean {
                zoomBy(detector.scaleFactor, detector.focusX, detector.focusY)
                return true
            }

            override fun onScaleEnd(detector: ScaleGestureDetector) {
                scaling = false
            }
        })

    private val gestureDetector =
        GestureDetector(context, object : GestureDetector.SimpleOnGestureListener() {
            override fun onDown(e: MotionEvent): Boolean = true

            override fun onScroll(
                e1: MotionEvent?,
                e2: MotionEvent,
                distanceX: Float,
                distanceY: Float
            ): Boolean {
                // 双指缩放过程中 GestureDetector 也会收到 scroll，忽略避免抖动。
                if (scaling || scaleDetector.isInProgress) return false
                if (currentScale <= fitScale + 0.001f) return false
                postTranslate(-distanceX, -distanceY)
                return true
            }

            override fun onDoubleTap(e: MotionEvent): Boolean {
                if (currentScale > fitScale + 0.001f) {
                    resetZoom()
                } else {
                    zoomBy(2.5f, e.x, e.y)
                }
                return true
            }

            override fun onSingleTapConfirmed(e: MotionEvent): Boolean {
                onImageClick?.invoke()
                return true
            }
        })

    init {
        scaleType = ScaleType.MATRIX
    }

    override fun setImageDrawable(drawable: Drawable?) {
        super.setImageDrawable(drawable)
        if (width > 0 && height > 0) resetZoom()
    }

    override fun onSizeChanged(w: Int, h: Int, oldw: Int, oldh: Int) {
        super.onSizeChanged(w, h, oldw, oldh)
        resetZoom()
    }

    /** 还原到 FIT_CENTER 效果（完整可见、居中）。 */
    fun resetZoom() {
        val d = drawable ?: return
        if (width <= 0 || height <= 0 || d.intrinsicWidth <= 0 || d.intrinsicHeight <= 0) return
        baseMatrix.reset()
        val src = RectF(0f, 0f, d.intrinsicWidth.toFloat(), d.intrinsicHeight.toFloat())
        val dst = RectF(0f, 0f, width.toFloat(), height.toFloat())
        baseMatrix.setRectToRect(src, dst, Matrix.ScaleToFit.CENTER)
        baseMatrix.getValues(values)
        fitScale = values[Matrix.MSCALE_X]
        currentScale = fitScale
        imageMatrix = baseMatrix
        invalidate()
    }

    private fun zoomBy(factor: Float, focusX: Float, focusY: Float) {
        val d = drawable ?: return
        val maxScale = fitScale * maxScaleMultiple
        val target = (currentScale * factor).coerceIn(fitScale, maxScale)
        val realFactor = target / currentScale
        if (realFactor == 1f) return
        currentScale = target
        workMatrix.set(imageMatrix)
        workMatrix.postScale(realFactor, realFactor, focusX, focusY)
        applyMatrix(workMatrix, d)
    }

    private fun postTranslate(dx: Float, dy: Float) {
        val d = drawable ?: return
        workMatrix.set(imageMatrix)
        workMatrix.postTranslate(dx, dy)
        applyMatrix(workMatrix, d)
    }

    private fun applyMatrix(matrix: Matrix, d: Drawable) {
        matrix.getValues(values)
        val displayedW = d.intrinsicWidth * values[Matrix.MSCALE_X]
        val displayedH = d.intrinsicHeight * values[Matrix.MSCALE_Y]
        var tx = values[Matrix.MTRANS_X]
        var ty = values[Matrix.MTRANS_Y]
        // 放大后拖到边缘即停；小于视图时始终居中，不露出底板空白。
        tx = if (displayedW >= width) tx.coerceIn(width - displayedW, 0f)
        else (width - displayedW) / 2f
        ty = if (displayedH >= height) ty.coerceIn(height - displayedH, 0f)
        else (height - displayedH) / 2f
        matrix.postTranslate(tx - values[Matrix.MTRANS_X], ty - values[Matrix.MTRANS_Y])
        imageMatrix = matrix
        invalidate()
    }

    override fun onTouchEvent(event: MotionEvent): Boolean {
        if (drawable == null) return false
        // 未放大时放行，父 ScrollView/Pager 正常滚页；放大或双指时自己接管。
        parent?.requestDisallowInterceptTouchEvent(
            currentScale > fitScale + 0.001f || event.pointerCount > 1
        )
        var handled = scaleDetector.onTouchEvent(event)
        handled = gestureDetector.onTouchEvent(event) || handled
        return handled || super.onTouchEvent(event)
    }
}
