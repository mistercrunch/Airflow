/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*! For license information please see main.dfca1a45df456ea33163.js.LICENSE.txt */
!function(e,t){"object"==typeof exports&&"object"==typeof module?module.exports=t():"function"==typeof define&&define.amd?define([],t):"object"==typeof exports?exports.Airflow=t():(e.Airflow=e.Airflow||{},e.Airflow.main=t())}(self,(()=>(()=>{"use strict";var e={d:(t,o)=>{for(var a in o)e.o(o,a)&&!e.o(t,a)&&Object.defineProperty(t,a,{enumerable:!0,get:o[a]})},o:(e,t)=>Object.prototype.hasOwnProperty.call(e,t),r:e=>{"undefined"!=typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(e,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(e,"__esModule",{value:!0})}},t={};return(()=>{e.r(t),e.d(t,{convertSecsToHumanReadable:()=>d,escapeHtml:()=>s});const o="YYYY-MM-DD, HH:mm:ss",a="YYYY-MM-DD, HH:mm:ss z",n="z (Z)";function r(e){return e instanceof moment?e.isUTC()?"UTC":e.format(n):"UTC"===e?e:moment().tz(e).format(n)}function i(e){moment.tz.setDefault(e),$('time[data-datetime-convert!="false"]').each(((e,t)=>{const n=$(t),r=moment(n.attr("datetime"));r._isValid&&n.text(r.format(n.data("with-tz")?a:o)),void 0!==n.attr("title")&&n.attr("title",r.isUTC()?"":`UTC: ${r.clone().utc().format()}`)})),$(".datetime input").each(((e,t)=>{t.value=moment(t.value).format()}))}function m(){const e=moment();$("#clock").attr("datetime",e.format("YYYY-MM-DDThh:mm:ssZ")).html(`${e.format("HH:mm")} <strong>${r(e)}</strong>`)}function l(e){localStorage.setItem("selected-timezone",e);const t=new CustomEvent("timezone",{detail:e});document.dispatchEvent(t),i(e),m(),$("body").trigger({type:"airflow.timezone-change",timezone:e})}window.isoDateToTimeEl=function(e,t){const a=moment(e),n=$.extend({title:!0},t).title,r=document.createElement("time");return r.setAttribute("datetime",a.format()),n&&r.setAttribute("title",a.isUTC()?"":`UTC: ${a.clone().utc().format()}`),r.innerText=a.format(o),r},window.moment=Airflow.moment;const c=document.createElement("span");function s(e){return c.textContent=e,c.innerHTML}function d(e){let t=e;const o=e,a=o-Math.floor(o);t=Math.floor(t);const n=Math.floor(t/3600);t-=3600*n;const r=Math.floor(t/60);t-=60*r;let i="";return n>0&&(i+=`${n}Hours `),r>0&&(i+=`${r}Min `),t+a>0&&(Math.floor(o)===o?i+=`${t}Sec`:(t+=a,i+=`${t.toFixed(3)}Sec`)),i}function u(){const e=moment.tz.guess(),t=localStorage.getItem("selected-timezone"),o=localStorage.getItem("chosen-timezone");function a(t){localStorage.setItem("chosen-timezone",t),t!==e||t!==Airflow.serverTimezone?($("#timezone-manual a").data("timezone",t).text(r(t)),$("#timezone-manual").show()):$("#timezone-manual").hide()}o&&a(o),l(t||Airflow.defaultUITimezone),"UTC"!==Airflow.serverTimezone&&($("#timezone-server a").html(`${r(Airflow.serverTimezone)} <span class="label label-primary">Server</span>`),$("#timezone-server").show()),Airflow.serverTimezone!==e?$("#timezone-local a").attr("data-timezone",e).html(`${r(e)} <span class="label label-info">Local</span>`):$("#timezone-local").hide(),$("a[data-timezone]").click((e=>{l($(e.currentTarget).data("timezone"))}));const n=moment.tz.names().map((e=>({category:e.split("/",1)[0],label:e.replace("_"," "),value:e})));$("#timezone-other").autocomplete({source:(e,t)=>{t(function(e,t){const o=new RegExp($.ui.autocomplete.escapeRegex(t),"i");return $.grep(e,(e=>o.test(e.label)||o.test(e.category)))}(n,e.term))},appendTo:"#timezone-menu > li:nth-child(6) > form",focus:(e,t)=>{e.preventDefault(),$(this).val(t.item.label)},select:(e,t)=>($(this).val(""),a(t.item.value),l(t.item.value),!1)}).data("ui-autocomplete")._renderItem=function(e,t){const o=$("<li>");return o.append(`<a class='dropdown-item' href='#' role='option'>${t.label}</a>`),o.appendTo(e)},$.ui.autocomplete.prototype._renderMenu=function(e,t){let o="";e.addClass("typeahead dropdown-menu"),e.attr("role","listbox"),$.each(t,((t,a)=>{a.category!==o&&(e.append(`<li class='ui-autocomplete-category dropdown-header'>${a.category}</li>`),o=a.category),this._renderItemData(e,a)}))}}function f(e){const t=$(e),o=$(".filter_val.form-control",t.parents("tr"));"Is Null"===t.text()||"Is not Null"===t.text()?(void 0!==o.attr("required")&&(o.removeAttr("required"),o.attr("airflow-required",!0)),1===o.parent(".datetime").length?o.parent(".datetime").hide():o.hide()):("true"===o.attr("airflow-required")&&(o.attr("required",!0),o.removeAttr("airflow-required")),1===o.parent(".datetime").length?o.parent(".datetime").show():o.show())}window.escapeHtml=s,window.convertSecsToHumanReadable=d,window.postAsForm=function(e,t){const o=$("<form></form>");o.attr("method","POST"),o.attr("action",e),$.each(t||{},((e,t)=>{const a=$("<input></input>");a.attr("type","hidden"),a.attr("name",e),a.attr("value",t),o.append(a)}));const a=$("<input></input>");a.attr("type","hidden"),a.attr("name","csrf_token"),a.attr("value",csrfToken),o.append(a),$(document.body).append(o),o.submit()},$(document).ready((()=>{u(),$("#clock").attr("data-original-title",hostName).attr("data-placement","bottom").parent().show(),m(),setInterval(m,1e3),$.ajaxSetup({beforeSend(e,t){/^(GET|HEAD|OPTIONS|TRACE)$/i.test(t.type)||this.crossDomain||e.setRequestHeader("X-CSRFToken",csrfToken)}}),$.fn.datetimepicker.defaults.sideBySide=!0,$(".datetimepicker").datetimepicker({format:"YYYY-MM-DDTHH:mm:ssZ"}),$(".datepicker").datetimepicker({format:"YYYY-MM-DD"}),$(".timepicker").datetimepicker({format:"HH:mm:ss"}),$(".filters .select2-chosen").each(((e,t)=>{f(t)})),$(".filters .select2-chosen").on("DOMNodeInserted",(e=>{f(e.target)})),$("#filter_form a.filter").click((()=>{$(".datetimepicker").datetimepicker(),$(".filters .select2-chosen").on("DOMNodeInserted",(e=>{f(e.target)}))})),$(".js-tooltip").tooltip()}))})(),t})()));
