!function(t){"object"==typeof exports&&"object"==typeof module?t(require("../../lib/codemirror")):"function"==typeof define&&define.amd?define(["../../lib/codemirror"],t):t(CodeMirror)}((function(t){"use strict";var e="CodeMirror-lint-markers";function n(t){t.parentNode&&t.parentNode.removeChild(t)}function o(e,o,i,r){var a=function(e,n,o){var i=document.createElement("div");function r(e){if(!i.parentNode)return t.off(document,"mousemove",r);var n=Math.max(0,e.clientY-i.offsetHeight-5),o=Math.max(0,Math.min(e.clientX+5,i.ownerDocument.defaultView.innerWidth-i.offsetWidth));i.style.top=n+"px",i.style.left=o+"px"}return i.className="CodeMirror-lint-tooltip cm-s-"+e.options.theme,i.appendChild(o.cloneNode(!0)),e.state.lint.options.selfContain?e.getWrapperElement().appendChild(i):document.body.appendChild(i),t.on(document,"mousemove",r),r(n),null!=i.style.opacity&&(i.style.opacity=1),i}(e,o,i);function l(){var e;t.off(r,"mouseout",l),a&&((e=a).parentNode&&(null==e.style.opacity&&n(e),e.style.opacity=0,setTimeout((function(){n(e)}),600)),a=null)}var s=setInterval((function(){if(a)for(var t=r;;t=t.parentNode){if(t&&11==t.nodeType&&(t=t.host),t==document.body)return;if(!t){l();break}}if(!a)return clearInterval(s)}),400);t.on(r,"mouseout",l)}function i(t,e,n){for(var i in this.marked=[],e instanceof Function&&(e={getAnnotations:e}),e&&!0!==e||(e={}),this.options={},this.linterOptions=e.options||{},r)this.options[i]=r[i];for(var i in e)r.hasOwnProperty(i)?null!=e[i]&&(this.options[i]=e[i]):e.options||(this.linterOptions[i]=e[i]);this.timeout=null,this.hasGutter=n,this.onMouseOver=function(e){!function(t,e){var n=e.target||e.srcElement;if(!/\bCodeMirror-lint-mark-/.test(n.className))return;for(var i=n.getBoundingClientRect(),r=(i.left+i.right)/2,a=(i.top+i.bottom)/2,l=t.findMarksAt(t.coordsChar({left:r,top:a},"client")),u=[],c=0;c<l.length;++c){var f=l[c].__annotation;f&&u.push(f)}u.length&&function(t,e,n){for(var i=n.target||n.srcElement,r=document.createDocumentFragment(),a=0;a<e.length;a++){var l=e[a];r.appendChild(s(l))}o(t,n,r,i)}(t,u,e)}(t,e)},this.waitingFor=0}var r={highlightLines:!1,tooltips:!0,delay:500,lintOnChange:!0,getAnnotations:null,async:!1,selfContain:null,formatAnnotation:null,onUpdateLinting:null};function a(t){var n=t.state.lint;n.hasGutter&&t.clearGutter(e),n.options.highlightLines&&function(t){t.eachLine((function(e){var n=e.wrapClass&&/\bCodeMirror-lint-line-\w+\b/.exec(e.wrapClass);n&&t.removeLineClass(e,"wrap",n[0])}))}(t);for(var o=0;o<n.marked.length;++o)n.marked[o].clear();n.marked.length=0}function l(e,n,i,r,a){var l=document.createElement("div"),s=l;return l.className="CodeMirror-lint-marker CodeMirror-lint-marker-"+i,r&&((s=l.appendChild(document.createElement("div"))).className="CodeMirror-lint-marker CodeMirror-lint-marker-multiple"),0!=a&&t.on(s,"mouseover",(function(t){o(e,t,n,s)})),l}function s(t){var e=t.severity;e||(e="error");var n=document.createElement("div");return n.className="CodeMirror-lint-message CodeMirror-lint-message-"+e,void 0!==t.messageHTML?n.innerHTML=t.messageHTML:n.appendChild(document.createTextNode(t.message)),n}function u(e){var n=e.state.lint;if(n){var o=n.options,i=o.getAnnotations||e.getHelper(t.Pos(0,0),"lint");if(i)if(o.async||i.async)!function(e,n){var o=e.state.lint,i=++o.waitingFor;function r(){i=-1,e.off("change",r)}e.on("change",r),n(e.getValue(),(function(n,a){e.off("change",r),o.waitingFor==i&&(a&&n instanceof t&&(n=a),e.operation((function(){c(e,n)})))}),o.linterOptions,e)}(e,i);else{var r=i(e.getValue(),n.linterOptions,e);if(!r)return;r.then?r.then((function(t){e.operation((function(){c(e,t)}))})):e.operation((function(){c(e,r)}))}}}function c(t,n){var o=t.state.lint;if(o){var i=o.options;a(t);for(var r,u,c=function(t){for(var e=[],n=0;n<t.length;++n){var o=t[n],i=o.from.line;(e[i]||(e[i]=[])).push(o)}return e}(n),f=0;f<c.length;++f){var m=c[f];if(m){for(var d=null,p=o.hasGutter&&document.createDocumentFragment(),h=0;h<m.length;++h){var g=m[h],v=g.severity;v||(v="error"),u=v,d="error"==(r=d)?r:u,i.formatAnnotation&&(g=i.formatAnnotation(g)),o.hasGutter&&p.appendChild(s(g)),g.to&&o.marked.push(t.markText(g.from,g.to,{className:"CodeMirror-lint-mark CodeMirror-lint-mark-"+v,__annotation:g}))}o.hasGutter&&t.setGutterMarker(f,e,l(t,p,d,m.length>1,i.tooltips)),i.highlightLines&&t.addLineClass(f,"wrap","CodeMirror-lint-line-"+d)}}i.onUpdateLinting&&i.onUpdateLinting(n,c,t)}}function f(t){var e=t.state.lint;e&&(clearTimeout(e.timeout),e.timeout=setTimeout((function(){u(t)}),e.options.delay))}t.defineOption("lint",!1,(function(n,o,r){if(r&&r!=t.Init&&(a(n),!1!==n.state.lint.options.lintOnChange&&n.off("change",f),t.off(n.getWrapperElement(),"mouseover",n.state.lint.onMouseOver),clearTimeout(n.state.lint.timeout),delete n.state.lint),o){for(var l=n.getOption("gutters"),s=!1,c=0;c<l.length;++c)l[c]==e&&(s=!0);var m=n.state.lint=new i(n,o,s);m.options.lintOnChange&&n.on("change",f),0!=m.options.tooltips&&"gutter"!=m.options.tooltips&&t.on(n.getWrapperElement(),"mouseover",m.onMouseOver),u(n)}})),t.defineExtension("performLint",(function(){u(this)}))}));